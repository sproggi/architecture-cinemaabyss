package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"

	"api-gateway/internal/config"
	"api-gateway/internal/handlers"
	"api-gateway/internal/middleware"
)

func main() {
	cfg := config.Load()

	r := chi.NewRouter()

	r.Use(middleware.Logging)
	r.Use(middleware.Recoverer)
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	r.Use(middleware.RateLimit(cfg.RateLimit, cfg.RateBurst))

	// Health check
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok","service":"api-gateway"}`))
	})
	r.Get("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"service":"api-gateway","uptime":"healthy"}`))
	})

	proxy := handlers.NewProxy(cfg)

	// Публичные маршруты (без JWT)
	r.Post("/api/auth/login", proxy.LoginHandler())
	r.Post("/api/v1/auth/login", proxy.LoginHandler())

	// ============================================
	// ПУБЛИЧНЫЕ МАРШРУТЫ (БЕЗ JWT) - /api/*
	// ============================================
	// ВАЖНО: здесь НЕТ r.Use(middleware.JWTAuth(...))
	r.Route("/api", func(r chi.Router) {
		// Монолит
		r.Mount("/users", proxy.Monolith())
		r.Mount("/subscriptions", proxy.Monolith())
		r.Mount("/payments", proxy.Monolith())
		r.Mount("/favorites", proxy.Monolith())

		// Movies – с постепенной миграцией
		r.Mount("/movies", proxy.Movies())

		// Events
		r.Mount("/events", proxy.Events())
	})

	// ============================================
	// ЗАЩИЩЁННЫЕ МАРШРУТЫ (С JWT) - /api/v1/*
	// ============================================
	r.Route("/api/v1", func(r chi.Router) {
		r.Use(middleware.JWTAuth(cfg.JWTSecret))

		r.Mount("/users", proxy.Monolith())
		r.Mount("/subscriptions", proxy.Monolith())
		r.Mount("/payments", proxy.Monolith())
		r.Mount("/favorites", proxy.Monolith())
		r.Mount("/movies", proxy.Movies())
		r.Mount("/events", proxy.Events())
	})

	// Запуск HTTP-сервера
	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: r,
	}

	go func() {
		log.Printf("API Gateway starting on port %s", cfg.Port)
		log.Printf("Routes:")
		log.Printf("  GET  /api/movies        -> Movies Service (PUBLIC)")
		log.Printf("  GET  /api/users         -> Monolith (PUBLIC)")
		log.Printf("  GET  /api/v1/movies     -> Movies Service (JWT required)")
		log.Printf("  GET  /api/v1/users      -> Monolith (JWT required)")
		log.Printf("  POST /api/auth/login    -> Login (no JWT)")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down API Gateway...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Shutdown error: %v", err)
	}
	log.Println("API Gateway stopped")
}