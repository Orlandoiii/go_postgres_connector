package main

import (
	"context"
	"fmt"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/app"
)

func replicate() {
	ctx := context.Background()

	connector, err := app.NewConnector(ctx)
	if err != nil {
		panic(fmt.Sprintf("error creating connector: %v", err))
	}
	defer connector.Close(ctx)

	if err := connector.Start(ctx); err != nil {
		panic(fmt.Sprintf("error starting connector: %v", err))
	}
}

func main() {

	fmt.Println("Starting replication...")
	replicate()
	fmt.Println("Replication started")
}
