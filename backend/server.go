package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/go-chi/cors"
)

func StartHTTPServer() {
	c := cors.New(cors.Options{
		AllowedOrigins:   []string{"http://example.com", "http://localhost:3000"},
		AllowCredentials: true,
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type"},
	})

	handler := c.Handler(http.HandlerFunc(handleData))

	http.Handle("/data", handler)
	log.Println("Starting HTTP server on :8787")
	if err := http.ListenAndServe(":8787", nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v\n", err)
	}
}

func handleData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request RequestData
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	mu.Lock()
	data, err := ReadParquets()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	response := ResponseData{
		Total: len(data),
		Page:  request.Page,
		Limit: request.Limit,
		Data:  data[request.Page*request.Limit : request.Page*request.Limit+request.Limit],
	}
	json.NewEncoder(w).Encode(response)
}
