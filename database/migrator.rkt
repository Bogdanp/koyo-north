#lang racket/base

(require component
         db
         koyo/database
         north
         north/adapter/base
         north/adapter/postgres
         north/adapter/sqlite
         racket/contract
         racket/match)

(provide
 migrator?
 make-migrator-factory)

(struct migrator (path db)
  #:transparent
  #:methods gen:component
  [(define (component-start m)
     (begin0 m
       (migrate! m)))

   (define (component-stop m)
     (struct-copy migrator m [db #f]))])

(define/contract ((make-migrator-factory path) db)
  (-> path? (-> database? migrator?))
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
        (define base (path->migration path))
        (define adapter
          (match (dbsystem-name (connection-dbsystem conn))
            ['sqlite3 (sqlite-adapter conn)]
            ['postgresql (postgres-adapter conn)]
            [name (error 'migrate "db system not supported: ~a" name)]))

        (adapter-init adapter)
        (define current (adapter-current-revision adapter))
        (define target (migration-revision (migration-most-recent base)))
        (for ([migration (in-list (migration-plan base current target))])
          (adapter-apply! adapter (migration-revision migration) (migration-up migration)))))))
