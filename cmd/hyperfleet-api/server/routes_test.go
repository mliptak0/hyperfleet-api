package server

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/gomega"
)

// minimalValidSchema is the smallest valid schema accepted by NewSchemaValidator.
const minimalValidSchema = `
openapi: 3.0.0
info:
  title: Test
  version: 1.0.0
paths: {}
components:
  schemas:
    ClusterSpec:
      type: object
    NodePoolSpec:
      type: object
`

func TestLoadSchemaValidator_EmptyPath(t *testing.T) {
	RegisterTestingT(t)

	v, err := loadSchemaValidator("")
	Expect(err).To(BeNil(), "empty path should not produce an error")
	Expect(v).To(BeNil(), "empty path should return nil validator (validation disabled)")
}

func TestLoadSchemaValidator_ValidPath(t *testing.T) {
	RegisterTestingT(t)

	schemaPath := writeSchemaFile(t, minimalValidSchema)

	v, err := loadSchemaValidator(schemaPath)
	Expect(err).To(BeNil())
	Expect(v).ToNot(BeNil(), "valid schema path should return a non-nil validator")
}

func TestLoadSchemaValidator_MissingFile(t *testing.T) {
	RegisterTestingT(t)

	_, err := loadSchemaValidator("/nonexistent/path/schema.yaml")
	Expect(err).ToNot(BeNil(), "missing file with non-empty path should return an error")
	Expect(err.Error()).To(ContainSubstring("failed to load OpenAPI schema"))
}

func TestLoadSchemaValidator_InvalidSchema(t *testing.T) {
	RegisterTestingT(t)

	schemaPath := writeSchemaFile(t, "this: is: not: valid: openapi")

	_, err := loadSchemaValidator(schemaPath)
	Expect(err).ToNot(BeNil(), "invalid schema file should return an error")
}

func writeSchemaFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "schema.yaml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("failed to write schema file: %v", err)
	}
	return path
}
