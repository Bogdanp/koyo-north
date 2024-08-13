#lang racket/base

(require component
         koyo/database
         (only-in north/adapter/base
                  exn:fail:adapter-cause
                  exn:fail:adapter:migration-revision
                  exn:fail:adapter:migration?)
         (prefix-in north: north/migrate)
         racket/contract/base
         racket/match)

(provide
 migrator?
 (contract-out
  [make-migrator-factory (-> path? (-> database? migrator?))]))

(struct migrator (path db)
  #:transparent
  #:methods gen:component
  [(define (component-start m)
     (begin0 m
       (migrate! m)))

   (define (component-stop m)
     (struct-copy migrator m [db #f]))])

(define ((make-migrator-factory path) db)
  (unless (directory-exists? path)
    (error 'make-migrator "migrations path does not exist: ~a" path))
  (migrator path db))

(define (migrate! m)
  (match-define (migrator path db) m)
  (with-handlers ([exn:fail:adapter:migration?
                   (lambda (e)
                     (error 'migrate!
                            "failed to apply revision ~a\n  cause: ~a"
                            (exn:fail:adapter:migration-revision e)
                            (exn-message (exn:fail:adapter-cause e))))])
    (call-with-database-connection db
      (lambda (conn)
        (north:migrate conn path)))))
