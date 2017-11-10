package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/antontsv/backup/awsglacier"
	"github.com/antontsv/backup/cloud"
	"github.com/antontsv/backup/gpcs"
	ini "gopkg.in/ini.v1"
)

var usage = func() {
	fmt.Fprintf(os.Stderr, "usage %s [-r] [-c config] file1 ... bucketName:[/path] \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {

	type config struct {
		selected bool
		ini      *ini.Section
	}

	providers := map[string]*config{
		"amazon": {},
		"google": {},
	}

	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}

	provider := flag.String("p", strings.Join(names, ","), fmt.Sprintf("Cloud service provider names to use"))
	creds := flag.String("c", "backup.ini", "File with cloud account config")
	recursive := flag.Bool("r", false, "Recursively backup entire directories")
	flag.CommandLine.Usage = usage
	flag.Parse()

	num := flag.NArg()
	if num < 1 || flag.Arg(0) == "" {
		log.Fatalln("Missing source file/directory. This must be specified as a first parameter")
	} else if num < 2 {
		log.Fatalln("Missing target destination. This must be specified as a second parameter as follows 'bucketName:/some/optional/path'")
	}

	cfg, err := ini.InsensitiveLoad(*creds)
	if err != nil {
		log.Fatalf("Cannot read credentials file %s: %v\n", *creds, err)
	}

	for _, s := range cfg.Sections() {
		name := s.Name()
		if _, ok := providers[name]; ok {
			providers[name].ini = s
		}
	}

	if *provider != "" {
		for _, p := range strings.Split(*provider, ",") {
			name := strings.TrimSpace(p)
			if _, ok := providers[name]; !ok {
				log.Fatalf("Selected cloud provider '%s' is not supported\n", name)
			}
			providers[name].selected = true
		}
	}

	var parts []string

	dest := strings.SplitN(flag.Arg(num-1), ":", 2)
	bucket := dest[0]

	if len(dest) > 1 {
		for _, part := range strings.Split(dest[1], "/") {
			if part != "" {
				parts = append(parts, part)
			}
		}
	}

	path := strings.TrimPrefix(strings.Join(parts, "/")+"/", "/")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sign := make(chan os.Signal)
		signal.Notify(sign, os.Interrupt)
		s := <-sign
		fmt.Fprintf(os.Stderr, "Got signal: %v, canceling processes in flight...\n", s)
		cancel()
	}()

	files := make(chan string)
	go walkSources(ctx, flag.Args()[0:num-1], *recursive, files)

	backupers := make(map[string]cloud.Backuper)
	doBackup := false
	for name, cnf := range providers {
		if cnf.selected && cnf.ini != nil {
			switch name {
			case "google":
				gpc, err := gpcs.New(ctx, bucket, cnf.ini)
				if err != nil {
					log.Fatalf("Cannot init Google backup: %v", err)
				}
				backupers["Google"] = gpc
			case "amazon":
				glacier, err := awsglacier.New(ctx, bucket, cnf.ini)
				if err != nil {
					log.Fatalf("Cannot init Amazon backup: %v", err)
				}
				backupers["Amazon"] = glacier
			}
			doBackup = true
		}
	}
	if !doBackup {
		log.Fatalln("No backup can be done because no cloud providers are configured")
	}

	wg := &sync.WaitGroup{}
	for f := range files {
		statuses := make(map[string]chan string)
		for name, bak := range backupers {
			wg.Add(1)
			status := make(chan string)
			statuses[name] = status
			go upload(ctx, bak, f, path+f, status, wg)
		}
		wg.Add(1)
		go status(ctx, f, statuses, wg)
		wg.Wait()
	}

}

func status(ctx context.Context, file string, statuses map[string]chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	mu := &sync.Mutex{}
	var names []string
	final := make(map[string]string)
	for name := range statuses {
		names = append(names, name)
		final[name] = "-"
	}
	sort.Strings(names)

	numDots := 1
	maxDots := 3
	maxLen := 0
	print := func() {
		fmt.Printf("File: %s: ", file)
		mu.Lock()
		defer mu.Unlock()
		dots := strings.Repeat(".", numDots) + strings.Repeat(" ", maxDots-numDots)
		numDots++
		if numDots > maxDots {
			numDots = 1
		}
		length := 0
		for _, name := range names {
			v := final[name]
			if strings.HasPrefix(v, ".") {
				v = dots
			}
			s := fmt.Sprintf("[%s: %s] ", name, v)
			length += len(s)
			fmt.Print(s)
		}
		if maxLen < length {
			maxLen = length
		}
		fmt.Print(strings.Repeat(" ", maxLen-length), "\r")
	}

	go func() {
		for len(statuses) > 0 {
			for name, notify := range statuses {
				select {
				case msg, ok := <-notify:
					if !ok {
						delete(statuses, name)
						break
					}
					mu.Lock()
					final[name] = msg
					mu.Unlock()
				default:
					break
				}
			}
		}
		cancel()
	}()

	for {
		select {
		case <-time.After(300 * time.Millisecond):
			print()
		case <-ctx.Done():
			print()
			fmt.Println()
			return
		}
	}

}

func upload(ctx context.Context, worker cloud.Backuper, file string, dest string, status chan string, wg *sync.WaitGroup) {
	defer func() {
		close(status)
	}()
	defer wg.Done()
	status <- "."
	err := worker.Upload(ctx, file, dest)
	if err != nil {
		status <- fmt.Sprintf("ERR ❌ : %s ", err.Error())
	} else {
		status <- "OK ✅ "
	}
}

func walkSources(ctx context.Context, sources []string, recursive bool, files chan string) {
	defer close(files)
	for _, source := range sources {
		select {
		case <-ctx.Done():
			return
		default:
			info, err := os.Stat(source)
			if err != nil {
				log.Fatalf("cannot open source: %s\n", source)
			}

			if info.IsDir() {
				if !recursive {
					log.Fatalf("%s is a directory, please use -r to make recursive backup\n", source)
				}
				err = filepath.Walk(source, func(path string, f os.FileInfo, err error) error {
					if err == nil && !f.IsDir() && ctx.Err() == nil {
						files <- path
					}
					return err

				})
			} else if ctx.Err() == nil {
				files <- source
			}

			if err != nil {
				log.Fatalf("cannot read directory %s: %v\n", source, err)
			}
		}
	}
}
