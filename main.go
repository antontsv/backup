/*
Package backup provides simple file/directory backup to
remote cloud such as Google and Amazon.

Concurrent backup will be performed against all providers
enabled in backup configuration INI file
*/
package main

import (
	"bytes"
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
	"github.com/fatih/color"
	ini "gopkg.in/ini.v1"
)

var usage = func() {
	fmt.Fprintf(os.Stderr, "usage %s [-r] [-c config] file1 ... bucketName:[/path] \n", os.Args[0])
	flag.PrintDefaults()
}

type config struct {
	selected bool
	ini      *ini.Section
}

func main() {

	providers := map[string]*config{
		"amazon": {},
		"google": {},
	}

	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}

	provider := flag.String("p", strings.Join(names, ","), fmt.Sprintf("Cloud service provider names to use"))
	creds := flag.String("c", "", "File with cloud account config")
	recursive := flag.Bool("r", false, "Recursively backup entire directories")
	useColor := flag.Bool("color", false, "Whether to use color on provider names")
	flag.CommandLine.Usage = usage
	flag.Parse()

	color.NoColor = !*useColor
	num := flag.NArg()
	if num < 1 || flag.Arg(0) == "" {
		log.Fatalln("Missing source file/directory. This must be specified as a first parameter")
	} else if num < 2 {
		log.Fatalln("Missing target destination. This must be specified as a second parameter as follows 'bucketName:/some/optional/path'")
	}

	if *creds != "" {
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sign := make(chan os.Signal)
		signal.Notify(sign, os.Interrupt)
		s := <-sign
		fmt.Fprintf(os.Stderr, "Got signal: %v, canceling processes in flight...\n", s)
		cancel()
	}()

	files := make(chan srcFile)
	go walkSources(ctx, flag.Args()[0:num-1], *recursive, files)
	dest := flag.Arg(num - 1)
	info, err := os.Stat(flag.Arg(0))
	if num > 2 || (err == nil && *recursive && info.IsDir()) {
		dest += "/"
	}
	runBackups(ctx, dest, providers, files)

}

// parseDest parses bucket and destination directory/file name is provided
// Format is similar to scp utility, where we use bucket name instead of hostname, i.e.
// bucket_name:destination_file/directory_name
func parseDest(dest string) (bucket, path string) {
	var parts []string
	d := strings.SplitN(dest, ":", 2)
	bucket = d[0]

	end := ""

	if len(d) > 1 {
		for _, part := range strings.Split(d[1], "/") {
			if part != "" {
				parts = append(parts, part)
			}
		}
		if d[1] != "/" && strings.HasSuffix(dest, "/") {
			end = "/"
		}
	}

	return bucket, strings.TrimPrefix(strings.Join(parts, "/"), "/") + end
}

type srcFile struct {
	path string
	base string
}

func runBackups(ctx context.Context, dest string, providers map[string]*config, files chan srcFile) {
	bucket, path := parseDest(dest)
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
				backupers[googlePrint()] = gpc
			case "amazon":
				glacier, err := awsglacier.New(ctx, bucket, cnf.ini)
				if err != nil {
					log.Fatalf("Cannot init Amazon backup: %v", err)
				}
				backupers[amazonPrint()] = glacier
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
			dest := path
			base := strings.TrimPrefix(f.base, "/")
			if strings.HasSuffix(dest, "/") {
				dest = fmt.Sprintf("%s%s", dest, base)
			} else if len(dest) <= 0 {
				dest = base
			}
			go upload(ctx, bak, f.path, dest, status, wg)
		}
		wg.Add(1)
		go status(ctx, f.path, statuses, wg)
		wg.Wait()
	}
}

// status visualizes progress, successes & failures from multiple concurrent backups.
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

// upload lauches specific backup library and reports success/failure to a channel, that is read separately
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

func walkSources(ctx context.Context, sources []string, recursive bool, files chan srcFile) {
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

			prefix := ""
			idx := strings.LastIndex(source, "/")
			if idx > 0 {
				prefix = source[0:idx]
			}

			if info.IsDir() {
				if !recursive {
					log.Fatalf("%s is a directory, please use -r to make recursive backup\n", source)
				}
				err = filepath.Walk(source, func(path string, f os.FileInfo, err error) error {
					if err == nil && !f.IsDir() && ctx.Err() == nil {
						files <- srcFile{path: path, base: strings.TrimPrefix(path, prefix)}
					}
					return err

				})
			} else if ctx.Err() == nil {
				files <- srcFile{path: source, base: strings.TrimPrefix(source, prefix)}
			}

			if err != nil {
				log.Fatalf("cannot read directory %s: %v\n", source, err)
			}
		}
	}
}

func googlePrint() string {
	w := &bytes.Buffer{}
	b := color.New(color.FgBlue)
	r := color.New(color.FgRed)
	g := color.New(color.FgGreen)
	y := color.New(color.FgYellow)
	b.Fprint(w, "G")
	r.Fprint(w, "o")
	y.Fprint(w, "o")
	b.Fprint(w, "g")
	g.Fprint(w, "l")
	r.Fprint(w, "e")
	return string(w.Bytes())
}
func amazonPrint() string {
	return color.New(color.FgYellow).Sprint("Amazon")
}
