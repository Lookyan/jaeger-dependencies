package main

import (
	"context"
	"log"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/pkg/es/wrapper"
	"github.com/jaegertracing/jaeger/plugin/storage/es/dependencystore"
	"gopkg.in/olivere/elastic.v5"
)

const jaegerSpanPrefix string = "jaeger-span-"
const jaegerDepPrefix string = "jaeger-dependencies-"
const bulkReadSize = 500

type Process struct {
	ServiceName string `json:"serviceName"`
}

type Reference struct {
	RefType string `json:"refType"`
	SpanID  string `json:"spanID"`
	TraceID string `json:"traceID"`
}

type Span struct {
	TraceID    string      `json:"traceID"`
	SpanID     string      `json:"spanID"`
	Process    Process     `json:"process"`
	References []Reference `json:"references"`
}

func IncCount(m map[string]uint64, serviceFrom string, serviceTo string) {
	key := serviceFrom + ":" + serviceTo
	if _, ok := m[key]; ok {
		m[key] += 1
	} else {
		m[key] = 1
	}
}

func GenIndexNameWithPrefix(prefix string) string {
	return prefix + time.Now().Format("2006-01-02")
}

func main() {
	esUsername := os.Getenv("ES_USERNAME")
	esPassword := os.Getenv("ES_PASSWORD")
	client, err := elastic.NewSimpleClient(
		elastic.SetURL(os.Getenv("ES_HOST")),
		elastic.SetBasicAuth(esUsername, esPassword))
	if err != nil {
		log.Fatal(err.Error())
	}
	ctx := context.Background()

	esIndexPrefix := os.Getenv("ES_INDEX_PREFIX")
	if esIndexPrefix != "" {
		esIndexPrefix += ":"
	}

	_, err = client.DeleteIndex(GenIndexNameWithPrefix(esIndexPrefix + jaegerDepPrefix)).Do(ctx)
	if err != nil {
		log.Println("Warning", err.Error())
	}

	bulkProcessorService := elastic.NewBulkProcessorService(client)
	bulkProcessor, err := bulkProcessorService.Do(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}
	esClientWrapper := eswrapper.WrapESClient(client, bulkProcessor)

	bq := elastic.NewBoolQuery()
	bq.Must(elastic.NewTermQuery("tags.key", "span.kind"))
	bq.Must(elastic.NewTermQuery("tags.value", "server"))

	serverSpanQuery := elastic.NewNestedQuery("tags", bq)

	hasReferenceQuery := elastic.NewExistsQuery("references")
	hasReferencesNestedQuery := elastic.NewNestedQuery("references", hasReferenceQuery)
	fullQuery := elastic.NewBoolQuery()
	fullQuery.Must(hasReferencesNestedQuery, serverSpanQuery)

	searchService := client.Scroll(GenIndexNameWithPrefix(esIndexPrefix + jaegerSpanPrefix)).
		Type("span").
		Query(fullQuery).
		Scroll("2m").
		Size(bulkReadSize).
		IgnoreUnavailable(true)

	servicePairToCount := make(map[string]uint64)

	for {
		searchResult, err := searchService.Do(ctx)
		if err != nil {
			log.Println("End of stream of spans")
			break
		}

		var ttyp Span
		res := searchResult.Each(reflect.TypeOf(ttyp))
		for _, span := range res {
			if s, ok := span.(Span); ok {
				refSpanID := s.References[0].SpanID
				parentSpanQuery := elastic.NewTermQuery("spanID", refSpanID)
				searchSpanService := client.
					Search().
					Index(GenIndexNameWithPrefix(esIndexPrefix + jaegerSpanPrefix)).
					Type("span").
					Query(parentSpanQuery).
					IgnoreUnavailable(true)
				spanResult, err := searchSpanService.Do(ctx)
				if err != nil {
					log.Println(err.Error())
					continue
				}
				parentSpan := spanResult.Each(reflect.TypeOf(ttyp))
				if len(parentSpan) > 0 {
					pSpan := parentSpan[0].(Span)
					IncCount(
						servicePairToCount,
						pSpan.Process.ServiceName,
						s.Process.ServiceName)
				}
			}
		}
	}

	log.Printf("%#v", servicePairToCount)

	var dependencies []model.DependencyLink
	for s, count := range servicePairToCount {
		fromto := strings.SplitN(s, ":", 2)
		dependencies = append(
			dependencies,
			model.DependencyLink{
				Parent:    fromto[0],
				Child:     fromto[1],
				CallCount: count,
			},
		)
	}

	store := dependencystore.NewDependencyStore(esClientWrapper, nil, "")
	err = store.WriteDependencies(time.Now(), dependencies)
	if err != nil {
		log.Fatal(err.Error())
	}
	bulkProcessor.Flush()
}
