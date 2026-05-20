package outbox

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

func ensureMigrationsTable(db *sql.DB) error {
	_, err := db.Exec(`
        CREATE TABLE IF NOT EXISTS outbox_schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMPTZ DEFAULT NOW()
        );
    `)
	return err
}

func getAppliedVersions(db *sql.DB) (map[int]bool, error) {
	rows, err := db.Query(`SELECT version FROM outbox_schema_migrations ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[int]bool)
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		applied[v] = true
	}
	return applied, rows.Err()
}

func applyMigration(db *sql.DB, version int, sqlScript string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Выполняем SQL миграции
	if _, err := tx.Exec(sqlScript); err != nil {
		return fmt.Errorf("migration %d: %w", version, err)
	}

	// Записываем версию
	if _, err := tx.Exec(`INSERT INTO outbox_schema_migrations (version) VALUES ($1)`, version); err != nil {
		return fmt.Errorf("failed to record migration %d: %w", version, err)
	}

	return tx.Commit()
}

// RunMigrations применяет все новые миграции из встроенных файлов.
func RunMigrations(db *sql.DB) error {
	if err := ensureMigrationsTable(db); err != nil {
		return err
	}
	applied, err := getAppliedVersions(db)
	if err != nil {
		return err
	}

	// Прочитать все файлы миграций из встроенной FS
	entries, err := MigrationsFS.ReadDir("migrations")
	if err != nil {
		return err
	}

	type migration struct {
		version int
		name    string
		content string
	}
	var migrations []migration

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".up.sql") {
			continue
		}
		// Извлечь номер версии из имени файла (например, 001_...)
		var version int
		if n, err := fmt.Sscanf(entry.Name(), "%d_", &version); n != 1 || err != nil {
			continue
		}
		content, err := MigrationsFS.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return err
		}
		migrations = append(migrations, migration{
			version: version,
			name:    entry.Name(),
			content: string(content),
		})
	}

	// Сортируем по версии
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].version < migrations[j].version
	})

	// Применяем только новые
	for _, m := range migrations {
		if applied[m.version] {
			continue
		}
		if err := applyMigration(db, m.version, m.content); err != nil {
			return fmt.Errorf("failed to apply migration %s: %w", m.name, err)
		}
	}
	return nil
}
