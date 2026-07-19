package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"api-gateway/internal/config"
	"api-gateway/internal/middleware"
)

type Proxy struct {
	cfg        *config.Config
	httpClient *http.Client
}

func NewProxy(cfg *config.Config) *Proxy {
	return &Proxy{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (p *Proxy) Monolith() http.Handler {
	return p.proxyTo(p.cfg.MonolithURL)
}

func (p *Proxy) Movies() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.cfg.EnableGradualMigration {
			if rand.Intn(100) < p.cfg.MoviesMigrationPercent {
				p.proxyTo(p.cfg.MoviesServiceURL).ServeHTTP(w, r)
				return
			}
		}
		p.proxyTo(p.cfg.MonolithURL).ServeHTTP(w, r)
	})
}

func (p *Proxy) Events() http.Handler {
	return p.proxyTo(p.cfg.EventsServiceURL)
}

func (p *Proxy) LoginHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var creds struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}
		if creds.Username == "" || creds.Password == "" {
			http.Error(w, "Username and password required", http.StatusBadRequest)
			return
		}

		token, err := middleware.GenerateJWT(creds.Username, p.cfg.JWTSecret)
		if err != nil {
			http.Error(w, "Failed to generate token", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"token": token,
			"type":  "Bearer",
		})
	}
}

func (p *Proxy) proxyTo(targetURL string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target, err := url.Parse(targetURL)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid target URL: %v", err), http.StatusInternalServerError)
			return
		}

		proxy := httputil.NewSingleHostReverseProxy(target)

		proxy.Director = func(req *http.Request) {
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host
			req.Host = target.Host
			req.URL.Path = r.URL.Path
			req.URL.RawPath = r.URL.RawPath
			req.Header.Set("X-Forwarded-Host", r.Host)
			req.Header.Set("X-Forwarded-For", r.RemoteAddr)
			req.Header.Set("X-Original-URI", r.URL.Path)
			req.Header.Set("X-Proxy", "api-gateway")
			req.Body = r.Body
		}

		proxy.ErrorHandler = func(w http.ResponseWriter, req *http.Request, err error) {
			log.Printf("Proxy error: %v", err)
			http.Error(w, "Service unavailable", http.StatusBadGateway)
		}

		log.Printf("Proxying %s %s -> %s%s", r.Method, r.URL.Path, targetURL, r.URL.Path)
		proxy.ServeHTTP(w, r)
	})
}