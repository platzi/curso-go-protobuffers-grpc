package database

import (
	"context"
	"database/sql"
	"log"

	_ "github.com/lib/pq"
	"platzi.com/go/grpc/models"
)

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(url string) (*PostgresRepository, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}

	return &PostgresRepository{db}, nil
}

func (repo *PostgresRepository) GetStudent(ctx context.Context, id string) (*models.Student, error) {
	rows, err := repo.db.QueryContext(ctx, "SELECT id, name, age FROM students WHERE id = $1", id)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = rows.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	var student = models.Student{}
	for rows.Next() {
		if err = rows.Scan(&student.Id, &student.Name, &student.Age); err == nil {
			return &student, nil
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return &student, nil
}

func (repo *PostgresRepository) SetStudent(ctx context.Context, student *models.Student) error {
	_, err := repo.db.ExecContext(ctx, "INSERT INTO students(id, name, age) VALUES($1, $2, $3)", student.Id, student.Name, student.Age)
	return err
}

func (repo *PostgresRepository) GetTest(ctx context.Context, id string) (*models.Test, error) {
	rows, err := repo.db.QueryContext(ctx, "SELECT id, name FROM tests WHERE id = $1", id)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = rows.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	var test = models.Test{}
	for rows.Next() {
		if err = rows.Scan(&test.Id, &test.Name); err == nil {
			return &test, nil
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return &test, nil
}

func (repo *PostgresRepository) SetTest(ctx context.Context, test *models.Test) error {
	_, err := repo.db.ExecContext(ctx, "INSERT INTO tests(id, name) VALUES($1, $2)", test.Id, test.Name)
	return err
}

func (repo *PostgresRepository) SetQuestion(ctx context.Context, question *models.Question) error {
	_, err := repo.db.ExecContext(ctx, "INSERT INTO questions(id, answer, question, test_id) VALUES($1, $2, $3, $4)", question.Id, question.Answer, question.Question, question.TestId)
	return err
}

func (repo *PostgresRepository) SetEnrollment(ctx context.Context, enrollment *models.Enrollment) error {
	_, err := repo.db.ExecContext(ctx, "INSERT INTO enrollments(student_id, test_id) VALUES($1, $2)", enrollment.StudentId, enrollment.TestId)
	return err
}

func (repo *PostgresRepository) GetStudentsPerTest(ctx context.Context, testId string) ([]*models.Student, error) {
	rows, err := repo.db.QueryContext(ctx, "SELECT id, name, age FROM students WHERE id IN (SELECT student_id FROM enrollments WHERE test_id = $1)", testId)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = rows.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	var students []*models.Student
	for rows.Next() {
		var student = models.Student{}
		if err = rows.Scan(&student.Id, &student.Name, &student.Age); err == nil {
			students = append(students, &student)
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return students, nil
}

func (repo *PostgresRepository) GetQuestionsPerTest(ctx context.Context, testId string) ([]*models.Question, error) {
	rows, err := repo.db.QueryContext(ctx, "SELECT id, question FROM questions WHERE test_id = $1", testId)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = rows.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	var questions []*models.Question
	for rows.Next() {
		var question = models.Question{}
		if err = rows.Scan(&question.Id, &question.Question); err == nil {
			questions = append(questions, &question)
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return questions, nil
}
