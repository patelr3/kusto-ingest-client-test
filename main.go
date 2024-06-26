package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/Azure/azure-kusto-go/azkustoingest"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

const (
	// KustoURL is the URL of the Kusto cluster.
	KustoURL = "https://ravpateadx.eastus.kusto.windows.net"
)

// AuthType is the authentication mechanism to use when
// interacting with the Kusto cluster.
type AuthType int

const (
	BearerToken AuthType = iota // Use a user's bearer token (will prompt for login)
	Interactive                 // Uses your existing az login credentials (or prompts for login if needed)
)

// String returns the string representation of the AuthType.
func (a AuthType) String() string {
	switch a {
	case BearerToken:
		return "BearerToken"
	case Interactive:
		return "Interactive"
	default:
		return "Unknown"
	}
}

func main() {
	authType := BearerToken
	log.Println("Auth type: ", authType.String())

	// Prepare clients
	kcsb, err := getKustoConnStr(authType)
	if err != nil {
		panic(err)
	}

	client, err := getKustoClient(kcsb)
	if err != nil {
		panic(err)
	}

	defer client.Close()

	// Pass down connection string to ingest client and ingest data
	log.Println("Ingesting data...")
	if err := ingestData(kcsb); err != nil {
		panic(err)
	}

	// Pass down kusto client to data client and get data
	log.Println("Getting data...")
	if err := getData(client); err != nil {
		panic(err)
	}

	log.Println("Done.")
}

// getAzBearerToken gets a bearer token from Azure Active Directory.
func getAzBearerToken() (*azcore.AccessToken, error) {
	cred, err := azidentity.NewDeviceCodeCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to obtain a credential: %v", err)
	}

	token, err := cred.GetToken(context.Background(), policy.TokenRequestOptions{
		Scopes: []string{fmt.Sprintf("%s/.default", KustoURL)},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get a token: %v", err)
	}

	return &token, nil
}

// getKustoConnStr gets a connection string for Kusto using the given auth type.
func getKustoConnStr(authType AuthType) (*azkustodata.ConnectionStringBuilder, error) {

	switch authType {
	case BearerToken:
		accessToken, err := getAzBearerToken()
		if err != nil {
			return nil, err
		}

		return azkustodata.NewConnectionStringBuilder(KustoURL).WitAadUserToken(accessToken.Token), nil
	case Interactive:
		return azkustodata.NewConnectionStringBuilder(KustoURL).WithDefaultAzureCredential(), nil
	default:
		return nil, fmt.Errorf("invalid auth type: " + fmt.Sprint(authType))
	}
}

// getKustoClient gets a Kusto client using the given connection string builder.
func getKustoClient(kcsb *azkustodata.ConnectionStringBuilder) (*azkustodata.Client, error) {
	client, err := azkustodata.New(kcsb)

	if err != nil {
		return nil, fmt.Errorf("error creating kusto client: %w", err)
	}

	return client, nil
}

// ingestData ingests data into the ravpateTable of the ArcSqlTelemetry database.
func ingestData(kcsb *azkustodata.ConnectionStringBuilder) error {
	ctx := context.Background()
	ingestor, err := azkustoingest.New(kcsb, azkustoingest.WithDefaultDatabase("ArcSqlTelemetry"), azkustoingest.WithDefaultTable("ravpateTable"))

	if err != nil {
		return fmt.Errorf("error creating ingestor: %w", err)
	}

	// Don't forget to close the ingestor when you're done.
	defer ingestor.Close()

	// Add a row to the ingestor.
	currentTime := time.Now().UTC()

	// Kusto Cluster has the following:
	// - ArcSqlTelemetry database name
	// - ravpateTable table name
	// - ravpateTable has: Timestamp, FirstName, LastName as columns
	// Getting the current time and ingesting that into the table to test we have gotten it.
	ingestQuery := fmt.Sprintf(`.ingest inline into table ravpateTable <| %s,Sql,Isgood`,
		currentTime.Format(time.RFC3339))

	log.Println("Writing ingest query to ingest.kql...")
	log.Println("\t", ingestQuery)
	os.WriteFile("ingest.kql", []byte(ingestQuery), 0644)

	log.Println("Running ingest query now...")

	ingestOptions := []azkustoingest.FileOption{
		azkustoingest.DeleteSource(),
		azkustoingest.FlushImmediately(),
		azkustoingest.ReportResultToTable(),
	}

	status, err := ingestor.FromFile(ctx, "./ingest.kql", ingestOptions...)
	if err != nil {
		return fmt.Errorf("error ingesting data: %w", err)
	}

	err = <-status.Wait(ctx)
	if err != nil {
		return fmt.Errorf("error waiting for ingest: %w", err)
	}

	return nil
}

// getData gets the last 5 rows from the ravpateTable of the ArcSqlTelemetry database.
func getData(client *azkustodata.Client) error {
	ctx := context.Background()
	dataset, err := client.IterativeQuery(ctx, "ArcSqlTelemetry", kql.New("ravpateTable | order by Timestamp desc | take 5"))
	if err != nil {
		return fmt.Errorf("error querying dataset: %w", err)
	}

	// Don't forget to close the dataset when you're done.
	defer dataset.Close()

	primaryResult := <-dataset.Tables() // The first table in the dataset will be the primary results.

	// Make sure to check for errors.
	if primaryResult.Err() != nil {
		return fmt.Errorf("error getting primary result: %w", primaryResult.Err())
	}

	log.Println("Results:")
	log.Println()
	for rowResult := range primaryResult.Table().Rows() {
		if rowResult.Err() != nil {
			return fmt.Errorf("error getting row result: %w", rowResult.Err())
		}
		row := rowResult.Row()

		log.Println(row)
	}

	return nil
}
