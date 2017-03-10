// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fanin

import (
	"time"

	"golang.org/x/net/context"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/prometheus/prometheus/storage/remote"
)

// Queryable is a local.Queryable that reads from local and remote storage.
type Queryable struct {
	Local  promql.Queryable
	Remote *remote.Reader
}

// Querier implements local.Queryable.
func (q Queryable) Querier() (local.Querier, error) {
	localQuerier, err := q.Local.Querier()
	if err != nil {
		return nil, err
	}

	fq := querier{
		local:   localQuerier,
		remotes: q.Remote.Queriers(),
	}
	return fq, nil
}

type querier struct {
	local   local.Querier
	remotes []local.Querier
}

func (q querier) QueryRange(ctx context.Context, from, through model.Time, matchers ...*metric.LabelMatcher) ([]local.SeriesIterator, error) {
	return q.query(func(q local.Querier) ([]local.SeriesIterator, error) {
		return q.QueryRange(ctx, from, through, matchers...)
	})
}

func (q querier) QueryInstant(ctx context.Context, ts model.Time, stalenessDelta time.Duration, matchers ...*metric.LabelMatcher) ([]local.SeriesIterator, error) {
	return q.query(func(q local.Querier) ([]local.SeriesIterator, error) {
		return q.QueryInstant(ctx, ts, stalenessDelta, matchers...)
	})
}

func (q querier) query(qFn func(q local.Querier) ([]local.SeriesIterator, error)) ([]local.SeriesIterator, error) {
	if len(q.remotes) == 0 {
		// Skip potentially expensive merge logic if there are no remote queriers.
		return qFn(q.local)
	}

	fpToIt := map[model.Fingerprint]iterator{}
	for _, q := range append([]local.Querier{q.local}, q.remotes...) {
		its, err := qFn(q)
		if err != nil {
			return nil, err
		}
		mergeIterators(fpToIt, its)
	}

	its := make([]local.SeriesIterator, 0, len(fpToIt))
	for _, it := range fpToIt {
		its = append(its, it)
	}
	return its, nil
}

func (q querier) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matcherSets ...metric.LabelMatchers) ([]metric.Metric, error) {
	// TODO: implement querying metrics from remote storage.
	return q.local.MetricsForLabelMatchers(ctx, from, through, matcherSets...)
}

func (q querier) LastSampleForLabelMatchers(ctx context.Context, cutoff model.Time, matcherSets ...metric.LabelMatchers) (model.Vector, error) {
	// TODO: implement querying last samples from remote storage.
	return q.local.LastSampleForLabelMatchers(ctx, cutoff, matcherSets...)
}

func (q querier) LabelValuesForLabelName(ctx context.Context, ln model.LabelName) (model.LabelValues, error) {
	// TODO: implement querying label values from remote storage.
	return q.local.LabelValuesForLabelName(ctx, ln)
}

func (q querier) Close() error {
	for _, q := range append([]local.Querier{q.local}, q.remotes...) {
		if err := q.Close(); err != nil {
			return err
		}
	}
	return nil
}

// iterator is a SeriesIterator which merges query results for multiple SeriesIterators.
type iterator []local.SeriesIterator

func (its iterator) ValueAtOrBeforeTime(t model.Time) model.SamplePair {
	latest := model.ZeroSamplePair
	for _, it := range its {
		v := it.ValueAtOrBeforeTime(t)
		if v.Timestamp.After(latest.Timestamp) {
			latest = v
		}
	}
	return latest
}

func (its iterator) RangeValues(interval metric.Interval) []model.SamplePair {
	var values []model.SamplePair
	for _, it := range its {
		values = mergeSamples(values, it.RangeValues(interval))
	}
	return values
}

func (its iterator) Metric() metric.Metric {
	return its[0].Metric()
}

func (its iterator) Close() {
	for _, it := range its {
		it.Close()
	}
}

func mergeIterators(fpToIt map[model.Fingerprint]iterator, its []local.SeriesIterator) {
	for _, it := range its {
		fp := it.Metric().Metric.Fingerprint()
		if fpIts, ok := fpToIt[fp]; !ok {
			fpToIt[fp] = iterator{it}
		} else {
			fpToIt[fp] = append(fpIts, it)
		}
	}
}

// mergeSamples merges two lists of sample pairs and removes duplicate
// timestamps. It assumes that both lists are sorted by timestamp.
func mergeSamples(a, b []model.SamplePair) []model.SamplePair {
	result := make([]model.SamplePair, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].Timestamp < b[j].Timestamp {
			result = append(result, a[i])
			i++
		} else if a[i].Timestamp > b[j].Timestamp {
			result = append(result, b[j])
			j++
		} else {
			result = append(result, a[i])
			i++
			j++
		}
	}
	for ; i < len(a); i++ {
		result = append(result, a[i])
	}
	for ; j < len(b); j++ {
		result = append(result, b[j])
	}
	return result
}
