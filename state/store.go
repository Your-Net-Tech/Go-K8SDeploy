package state

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

type Deployment struct {
	ID          int64
	Project     string
	Revision    int
	Status      string
	Manifests   string
	Images      string
	StartedAt   time.Time
	FinishedAt  *time.Time
	Error       string
	RollbackOf  *int64
	CreatedAt   time.Time
}

type Build struct {
	ID          int64
	DeploymentID int64
	Service     string
	Dockerfile  string
	Context     string
	Image       string
	Status      string
	StartedAt   time.Time
	FinishedAt  *time.Time
	LogPath     string
	CreatedAt   time.Time
}

func NewStore(dataDir string) (*Store, error) {
	os.MkdirAll(dataDir, 0755)
	dbPath := filepath.Join(dataDir, "deploy.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS deployments (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		project TEXT NOT NULL,
		revision INTEGER NOT NULL,
		status TEXT NOT NULL,
		manifests TEXT,
		images TEXT,
		started_at DATETIME NOT NULL,
		finished_at DATETIME,
		error TEXT,
		rollback_of INTEGER,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	CREATE TABLE IF NOT EXISTS builds (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		deployment_id INTEGER NOT NULL,
		service TEXT NOT NULL,
		dockerfile TEXT NOT NULL,
		context TEXT NOT NULL,
		image TEXT NOT NULL,
		status TEXT NOT NULL,
		started_at DATETIME NOT NULL,
		finished_at DATETIME,
		log_path TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY (deployment_id) REFERENCES deployments(id)
	);
	CREATE INDEX IF NOT EXISTS idx_deploy_project ON deployments(project);
	CREATE INDEX IF NOT EXISTS idx_build_deployment ON builds(deployment_id);
	`
	_, err := s.db.Exec(schema)
	return err
}

func (s *Store) CreateDeployment(project string, manifests []string, images map[string]string) (*Deployment, error) {
	var lastRev int
	err := s.db.QueryRow("SELECT COALESCE(MAX(revision), 0) FROM deployments WHERE project = ?", project).Scan(&lastRev)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	manifestsJSON, _ := json.Marshal(manifests)
	imagesJSON, _ := json.Marshal(images)

	res, err := s.db.Exec(
		`INSERT INTO deployments (project, revision, status, manifests, images, started_at) VALUES (?, ?, ?, ?, ?, ?)`,
		project, lastRev+1, "pending", string(manifestsJSON), string(imagesJSON), time.Now(),
	)
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	return s.GetDeployment(id)
}

func (s *Store) GetDeployment(id int64) (*Deployment, error) {
	row := s.db.QueryRow(
		`SELECT id, project, revision, status, manifests, images, started_at, finished_at, error, rollback_of, created_at
		 FROM deployments WHERE id = ?`, id)
	d := &Deployment{}
	var finishedAt sql.NullTime
	var rollbackOf sql.NullInt64
	var errMsg sql.NullString
	var manifests, images string
	if err := row.Scan(&d.ID, &d.Project, &d.Revision, &d.Status, &manifests, &images, &d.StartedAt, &finishedAt, &errMsg, &rollbackOf, &d.CreatedAt); err != nil {
		return nil, err
	}
	if finishedAt.Valid { d.FinishedAt = &finishedAt.Time }
	if rollbackOf.Valid { d.RollbackOf = &rollbackOf.Int64 }
	if errMsg.Valid { d.Error = errMsg.String }
	json.Unmarshal([]byte(manifests), &d.Manifests)
	json.Unmarshal([]byte(images), &d.Images)
	return d, nil
}

func (s *Store) UpdateDeploymentStatus(id int64, status, errMsg string) error {
	_, err := s.db.Exec(`UPDATE deployments SET status = ?, error = ?, finished_at = ? WHERE id = ?`,
		status, errMsg, time.Now(), id)
	return err
}

func (s *Store) CreateBuild(deploymentID int64, service, dockerfile, context, image string) (*Build, error) {
	res, err := s.db.Exec(
		`INSERT INTO builds (deployment_id, service, dockerfile, context, image, status, started_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		deploymentID, service, dockerfile, context, image, "building", time.Now(),
	)
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	return s.GetBuild(id)
}

func (s *Store) GetBuild(id int64) (*Build, error) {
	row := s.db.QueryRow(
		`SELECT id, deployment_id, service, dockerfile, context, image, status, started_at, finished_at, log_path, created_at
		 FROM builds WHERE id = ?`, id)
	b := &Build{}
	var finishedAt sql.NullTime
	var logPath sql.NullString
	if err := row.Scan(&b.ID, &b.DeploymentID, &b.Service, &b.Dockerfile, &b.Context, &b.Image, &b.Status, &b.StartedAt, &finishedAt, &logPath, &b.CreatedAt); err != nil {
		return nil, err
	}
	if finishedAt.Valid { b.FinishedAt = &finishedAt.Time }
	if logPath.Valid { b.LogPath = logPath.String }
	return b, nil
}

func (s *Store) UpdateBuildStatus(id int64, status, logPath string) error {
	_, err := s.db.Exec(`UPDATE builds SET status = ?, log_path = ?, finished_at = ? WHERE id = ?`,
		status, logPath, time.Now(), id)
	return err
}

func (s *Store) GetBuildsByDeployment(deploymentID int64) ([]*Build, error) {
	rows, err := s.db.Query(
		`SELECT id, deployment_id, service, dockerfile, context, image, status, started_at, finished_at, log_path, created_at
		 FROM builds WHERE deployment_id = ?`, deploymentID)
	if err != nil { return nil, err }
	defer rows.Close()

	var builds []*Build
	for rows.Next() {
		b := &Build{}
		var finishedAt sql.NullTime
		var logPath sql.NullString
		if err := rows.Scan(&b.ID, &b.DeploymentID, &b.Service, &b.Dockerfile, &b.Context, &b.Image, &b.Status, &b.StartedAt, &finishedAt, &logPath, &b.CreatedAt); err != nil {
			return nil, err
		}
		if finishedAt.Valid { b.FinishedAt = &finishedAt.Time }
		if logPath.Valid { b.LogPath = logPath.String }
		builds = append(builds, b)
	}
	return builds, nil
}

func (s *Store) ListDeployments(project string, limit int) ([]*Deployment, error) {
	query := `SELECT id, project, revision, status, manifests, images, started_at, finished_at, error, rollback_of, created_at
		FROM deployments WHERE project = ? ORDER BY revision DESC LIMIT ?`
	rows, err := s.db.Query(query, project, limit)
	if err != nil { return nil, err }
	defer rows.Close()

	var deps []*Deployment
	for rows.Next() {
		d := &Deployment{}
		var finishedAt sql.NullTime
		if err := rows.Scan(&d.ID, &d.Project, &d.Revision, &d.Status, &d.Manifests, &d.Images, &d.StartedAt, &finishedAt, &d.Error, &d.RollbackOf, &d.CreatedAt); err != nil {
			return nil, err
		}
		if finishedAt.Valid { d.FinishedAt = &finishedAt.Time }
		deps = append(deps, d)
	}
	return deps, nil
}

func (s *Store) Close() error { return s.db.Close() }