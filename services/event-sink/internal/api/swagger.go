package api

import (
	"log"

	"github.com/swaggo/swag"
)

type SwaggerInfo struct{}

func (si *SwaggerInfo) ReadDoc() string {
	doc, err := GetSwagger()
	if err != nil {
		log.Fatalf("Error fetching Swagger spec: %v", err)
		return ""
	}
	doc.OpenAPI = "3.0.0"
	swaggerJSON, err := doc.MarshalJSON()
	if err != nil {
		log.Fatalf("Error marshaling Swagger spec: %v", err)
		return ""
	}
	return string(swaggerJSON)
}

func init() {
	swag.Register(swag.Name, &SwaggerInfo{})
}
