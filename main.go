package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	_ "github.com/anacrolix/envpprof"

	"github.com/anacrolix/tagflag"

	"github.com/anacrolix/missinggo/cache"

	"github.com/anacrolix/missinggo"
	"github.com/rjeczalik/notify"
	"golang.org/x/xerrors"
)

func fileInfoToStat(fi os.FileInfo, err error) Stat {
	if os.IsNotExist(err) {
		return Stat{}
	}
	if err != nil {
		return Stat{
			err: err,
		}
	}
	var s Stat
	s.lastUsed = missinggo.FileInfoAccessTime(fi)
	ss := fi.Sys().(*syscall.Stat_t)
	s.size = ss.Blocks * 512
	s.exists = true
	s.isDir = fi.IsDir()
	s.isSymlink = fi.Mode()&os.ModeSymlink == os.ModeSymlink
	return s
}

func logOsStat(name string, fi os.FileInfo, err error) {
	//log.Printf("%q: %#v", name, fi)
}

func stat(name string) Stat {
	fi, err := os.Stat(name)
	logOsStat(name, fi, err)
	return fileInfoToStat(fi, err)
}

type file struct {
	lastUsed  time.Time
	size      int64
	isDir     bool
	isSymlink bool
}

type Stat struct {
	file
	exists bool
	err    error
}

func mainErr() error {
	flags := struct {
		Capacity  tagflag.Bytes
		DryRun    bool
		Status    time.Duration
		LogEvents bool
		tagflag.StartPos
		Roots []string
	}{
		Capacity: math.MaxInt64,
		Status:   time.Minute,
	}
	tagflag.Parse(&flags)
	ec := make(chan notify.EventInfo, 1)
	defer notify.Stop(ec)
	deleteChan := make(chan struct{}, 1)
	var z zone
	z.Capacity = flags.Capacity.Int64()
	z.c.Policy = &cache.LruPolicy{}
	z.notify = func() {
		select {
		case deleteChan <- struct{}{}:
		default:
		}
	}
	go func() {
		for {
			log.Print(&z.c)
			time.Sleep(flags.Status)
		}
	}()
	go handleEvents(ec, &z, flags.LogEvents)
	var wg sync.WaitGroup
	for _, r := range flags.Roots {
		if err := notify.Watch(filepath.Join(r, "..."), ec, notify.All); err != nil {
			return xerrors.Errorf("error watching %q: %w", r, err)
		}
		wg.Add(1)
		go func(r string) {
			defer wg.Done()
			walk(r, func(name string, s Stat) {
				applyStat(&z, name, s)
			})
		}(r)
	}
	wg.Wait()
	log.Print(&z.c)
	go deleter(deleteChan, []*zone{&z}, func(name string) bool {
		log.Printf("deleting %q", name)
		if flags.DryRun {
			return true
		}
		err := os.Remove(name)
		if err != nil {
			log.Printf("error removing %q: %v", name, err)
		}
		return err == nil || os.IsNotExist(err)
	})
	select {}
	return nil
}

type zone struct {
	Capacity int64
	c        cache.Cache
	notify   func()
}

func (z *zone) checkFull() {
	z.notify()
}

func handleEvents(ec chan notify.EventInfo, z *zone, logEvents bool) {
	for ei := range ec {
		if logEvents {
			log.Printf("got event info: %v\n%#v", ei, ei.Sys())
		}
		s := stat(ei.Path())
		applyStat(z, ei.Path(), s)
	}
}

func walk(name string, f func(string, Stat)) {
	filepath.Walk(name, func(path string, fi os.FileInfo, err error) error {
		logOsStat(path, fi, err)
		f(path, fileInfoToStat(fi, err))
		return nil
	})
}

func applyStat(z *zone, name string, s Stat) {
	if s.err != nil {
		log.Printf("can't apply stat: %v", s.err)
	}
	if s.exists {
		var i cache.Item
		i.Key = name
		i.Size = s.size
		i.Usage = timeUsage(s.lastUsed)
		i.CanEvict = !s.isDir && !s.isSymlink
		z.c.Update(i)
	} else {
		z.c.Remove(name)
	}
	z.checkFull()
}

func main() {
	log.SetFlags(log.Flags() | log.Lshortfile)
	if err := mainErr(); err != nil {
		log.Fatalf("error in main: %v", err)
	}
}

type timeUsage time.Time

var _ interface {
	cache.Usage
	fmt.Formatter
} = timeUsage{}

func (me timeUsage) Less(other cache.Usage) bool {
	return time.Time(me).Before(time.Time(other.(timeUsage)))
}

func (me timeUsage) String() string {
	return time.Time(me).Format(time.RFC3339)
}

func (me timeUsage) Format(s fmt.State, c rune) {
	fmt.Fprint(s, time.Time(me).Format(time.RFC3339))
}

func deleter(check <-chan struct{}, zs []*zone, remover func(name string) bool) {
	for range check {
		for _, z := range zs {
			for z.c.Filled() > z.Capacity {
				evictInZone(z, zs, remover)
			}
		}
	}
}

func evictInZone(z *zone, notify []*zone, remover func(name string) bool) {
	name, ok := z.c.Policy.Candidate()
	if !ok {
		log.Printf("zone full but no removal candidates")
		return
	}
	if remover(name) {
		for _, z := range notify {
			z.c.Remove(name)
		}
	} else {
		s := stat(name)
		for _, z := range notify {
			applyStat(z, name, s)
		}
	}

}
