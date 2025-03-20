package main

func main() {

	// Start HTTP server.
	go StartHTTPServer()
	// watch for logs
	WatchForLogs()

}
