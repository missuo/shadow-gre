package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/missuo/shadow-gre/pkg/shadowgre"
)

var (
	version = "dev"
)

func main() {
	// Define flags
	mode := flag.String("mode", "", "Mode: client or server (required)")
	listen := flag.String("listen", "", "Listen address for TCP (client mode, e.g., 0.0.0.0:1080)")
	localIP := flag.String("local", "", "Local IP address for GRE binding (required)")
	remote := flag.String("remote", "", "Remote server IP (client mode, required)")
	backend := flag.String("backend", "", "Backend server address (server mode, e.g., 127.0.0.1:8388)")
	password := flag.String("password", "", "Shared password/key for the tunnel (required)")
	showVersion := flag.Bool("version", false, "Show version")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "shadow-gre - TCP over GRE tunnel\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  Client mode:\n")
		fmt.Fprintf(os.Stderr, "    shadow-gre -mode client -listen 0.0.0.0:1080 -local <local_ip> -remote <server_ip> -password <password>\n\n")
		fmt.Fprintf(os.Stderr, "  Server mode:\n")
		fmt.Fprintf(os.Stderr, "    shadow-gre -mode server -local <local_ip> -backend 127.0.0.1:8388 -password <password>\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nNote: This program requires root/sudo privileges to create raw sockets.\n")
	}

	flag.Parse()

	if *showVersion {
		fmt.Printf("shadow-gre version %s\n", version)
		os.Exit(0)
	}

	// Validate common flags
	if *mode == "" {
		fmt.Fprintln(os.Stderr, "Error: -mode is required (client or server)")
		flag.Usage()
		os.Exit(1)
	}

	if *localIP == "" {
		fmt.Fprintln(os.Stderr, "Error: -local is required")
		flag.Usage()
		os.Exit(1)
	}

	if *password == "" {
		fmt.Fprintln(os.Stderr, "Error: -password is required")
		flag.Usage()
		os.Exit(1)
	}

	local := net.ParseIP(*localIP)
	if local == nil {
		fmt.Fprintf(os.Stderr, "Error: invalid local IP: %s\n", *localIP)
		os.Exit(1)
	}

	// Generate key from password
	key := passwordToKey(*password)

	switch *mode {
	case "client":
		runClient(*listen, local, *remote, key)
	case "server":
		runServer(local, *backend, key)
	default:
		fmt.Fprintf(os.Stderr, "Error: invalid mode: %s (must be 'client' or 'server')\n", *mode)
		os.Exit(1)
	}
}

func runClient(listen string, local net.IP, remote string, key uint32) {
	if listen == "" {
		fmt.Fprintln(os.Stderr, "Error: -listen is required in client mode")
		os.Exit(1)
	}

	if remote == "" {
		fmt.Fprintln(os.Stderr, "Error: -remote is required in client mode")
		os.Exit(1)
	}

	remoteIP := net.ParseIP(remote)
	if remoteIP == nil {
		fmt.Fprintf(os.Stderr, "Error: invalid remote IP: %s\n", remote)
		os.Exit(1)
	}

	client := shadowgre.NewClient(listen, local, remoteIP, key)

	if err := client.Start(); err != nil {
		log.Fatalf("Failed to start client: %v", err)
	}

	// Wait for signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	client.Close()
}

func runServer(local net.IP, backend string, key uint32) {
	if backend == "" {
		fmt.Fprintln(os.Stderr, "Error: -backend is required in server mode")
		os.Exit(1)
	}

	server := shadowgre.NewServerGVisor(local, backend, key)

	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// Wait for signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	server.Close()
}

// passwordToKey converts a password string to a 32-bit key using FNV-1a hash
func passwordToKey(password string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(password))
	return h.Sum32()
}
