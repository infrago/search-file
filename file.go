package search_file

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bamgoo/bamgoo"
	. "github.com/bamgoo/base"
	"github.com/bamgoo/search"
	"github.com/blevesearch/bleve/v2"
	bmapping "github.com/blevesearch/bleve/v2/mapping"
	bsearch "github.com/blevesearch/bleve/v2/search"
	bquery "github.com/blevesearch/bleve/v2/search/query"
)

type fileDriver struct{}

type fileConnection struct {
	root    string
	prefix  string
	mu      sync.Mutex
	indexes map[string]bleve.Index
	defs    map[string]search.Index
}

func init() {
	bamgoo.Register("file", &fileDriver{})
}

func (d *fileDriver) Connect(inst *search.Instance) (search.Connection, error) {
	root := valueString(inst.Config.Setting, "path", "file", "db", "database")
	if root == "" {
		root = "store/search"
	}
	prefix := inst.Config.Prefix
	if prefix == "" {
		prefix = valueString(inst.Config.Setting, "prefix")
	}
	return &fileConnection{root: root, prefix: prefix, indexes: map[string]bleve.Index{}, defs: map[string]search.Index{}}, nil
}

func (c *fileConnection) Open() error {
	return os.MkdirAll(c.root, 0o755)
}

func (c *fileConnection) Capabilities() search.Capabilities {
	return search.Capabilities{
		SyncIndex: true,
		Clear:     true,
		Upsert:    true,
		Delete:    true,
		Search:    true,
		Count:     true,
		Suggest:   false,
		Sort:      true,
		Facets:    true,
		Highlight: true,
		FilterOps: []string{OpEq, OpIn, OpGt, OpGte, OpLt, OpLte, OpRange},
	}
}

func (c *fileConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, idx := range c.indexes {
		_ = idx.Close()
	}
	c.indexes = map[string]bleve.Index{}
	c.defs = map[string]search.Index{}
	return nil
}

func (c *fileConnection) SyncIndex(name string, index search.Index) error {
	c.mu.Lock()
	c.defs[name] = index
	c.mu.Unlock()
	_, err := c.ensure(name)
	return err
}

func (c *fileConnection) Clear(name string) error {
	c.mu.Lock()
	if idx, ok := c.indexes[name]; ok && idx != nil {
		_ = idx.Close()
		delete(c.indexes, name)
	}
	indexDef := c.defs[name]
	c.mu.Unlock()

	if err := os.RemoveAll(c.indexPath(name)); err != nil {
		return err
	}
	return c.SyncIndex(name, indexDef)
}

func (c *fileConnection) Upsert(index string, rows []Map) error {
	idx, err := c.ensure(index)
	if err != nil {
		return err
	}
	batch := idx.NewBatch()
	for _, row := range rows {
		if row == nil {
			continue
		}
		id := fmt.Sprintf("%v", row["id"])
		if id == "" || id == "<nil>" {
			continue
		}
		payload := cloneMap(row)
		raw, _ := json.Marshal(payload)
		docMap := map[string]any{"__id": id, "__payload": string(raw)}
		for k, v := range payload {
			docMap[k] = v
		}
		if err := batch.Index(id, docMap); err != nil {
			return err
		}
	}
	return idx.Batch(batch)
}

func (c *fileConnection) Delete(index string, ids []string) error {
	idx, err := c.ensure(index)
	if err != nil {
		return err
	}
	batch := idx.NewBatch()
	for _, id := range ids {
		batch.Delete(id)
	}
	return idx.Batch(batch)
}

func (c *fileConnection) Search(index string, query search.Query) (search.Result, error) {
	idx, err := c.ensure(index)
	if err != nil {
		return search.Result{}, err
	}

	q := buildQuery(query)
	req := bleve.NewSearchRequestOptions(q, query.Limit, query.Offset, false)
	req.Fields = []string{"__payload"}
	if len(query.Facets) > 0 {
		for _, field := range query.Facets {
			req.AddFacet(field, bleve.NewFacetRequest(field, 32))
		}
	}
	if len(query.Sorts) > 0 {
		sorts := make([]string, 0, len(query.Sorts))
		for _, s := range query.Sorts {
			if s.Desc {
				sorts = append(sorts, "-"+s.Field)
			} else {
				sorts = append(sorts, s.Field)
			}
		}
		req.SortBy(sorts)
	}
	if len(query.Highlight) > 0 {
		req.Highlight = bleve.NewHighlight()
	}

	resp, err := idx.Search(req)
	if err != nil {
		return search.Result{}, err
	}
	if strings.TrimSpace(query.Keyword) != "" && !query.Prefix && len(resp.Hits) == 0 {
		// Bleve default analyzers are weak on some CJK queries. For file-mode
		// small projects, fallback to full scan keeps behavior predictable.
		return c.fallbackSearch(idx, query)
	}

	hits := make([]search.Hit, 0, len(resp.Hits))
	for _, hit := range resp.Hits {
		payload := Map{}
		if raw, ok := hit.Fields["__payload"].(string); ok && raw != "" {
			_ = json.Unmarshal([]byte(raw), &payload)
		}
		if payload == nil {
			payload = Map{}
		}
		if len(query.Fields) > 0 {
			payload = pickFields(payload, query.Fields)
		}
		h := search.Hit{ID: hit.ID, Score: hit.Score, Payload: payload}
		if len(hit.Fragments) > 0 {
			for k, arr := range hit.Fragments {
				if len(arr) > 0 {
					h.Payload[k] = arr[0]
				}
			}
		}
		hits = append(hits, h)
	}

	facets := map[string][]search.Facet{}
	for field, f := range resp.Facets {
		terms := []*bsearch.TermFacet{}
		if f.Terms != nil {
			terms = f.Terms.Terms()
		}
		vals := make([]search.Facet, 0, len(terms))
		for _, t := range terms {
			vals = append(vals, search.Facet{Field: field, Value: t.Term, Count: int64(t.Count)})
		}
		facets[field] = vals
	}

	return search.Result{Total: int64(resp.Total), Took: int64(resp.Took), Hits: hits, Facets: facets}, nil
}

func (c *fileConnection) fallbackSearch(idx bleve.Index, query search.Query) (search.Result, error) {
	start := time.Now()
	keyword := strings.TrimSpace(query.Keyword)
	lowerKeyword := strings.ToLower(keyword)

	req := bleve.NewSearchRequestOptions(bleve.NewMatchAllQuery(), 10000, 0, false)
	req.Fields = []string{"__payload"}
	resp, err := idx.Search(req)
	if err != nil {
		return search.Result{}, err
	}

	matched := make([]search.Hit, 0)
	for _, hit := range resp.Hits {
		payload := Map{}
		if raw, ok := hit.Fields["__payload"].(string); ok && raw != "" {
			_ = json.Unmarshal([]byte(raw), &payload)
		}
		if payload == nil {
			payload = Map{}
		}
		rawText, _ := json.Marshal(payload)
		if lowerKeyword != "" && !strings.Contains(strings.ToLower(string(rawText)), lowerKeyword) {
			continue
		}
		pass := true
		for _, f := range query.Filters {
			if !search.FilterMatch(f, payload) {
				pass = false
				break
			}
		}
		if !pass {
			continue
		}
		matched = append(matched, search.Hit{ID: hit.ID, Score: 1, Payload: payload})
	}

	if len(query.Sorts) > 0 {
		sort.SliceStable(matched, func(i, j int) bool {
			for _, s := range query.Sorts {
				ai := matched[i].Payload[s.Field]
				aj := matched[j].Payload[s.Field]
				cmp := compareForSort(ai, aj)
				if cmp == 0 {
					continue
				}
				if s.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
			return matched[i].ID < matched[j].ID
		})
	}

	facets := map[string][]search.Facet{}
	for _, field := range query.Facets {
		counter := map[string]int64{}
		for _, hit := range matched {
			counter[fmt.Sprintf("%v", hit.Payload[field])]++
		}
		keys := make([]string, 0, len(counter))
		for k := range counter {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		arr := make([]search.Facet, 0, len(keys))
		for _, k := range keys {
			arr = append(arr, search.Facet{Field: field, Value: k, Count: counter[k]})
		}
		facets[field] = arr
	}

	total := int64(len(matched))
	offset := query.Offset
	if offset < 0 {
		offset = 0
	}
	limit := query.Limit
	if limit <= 0 {
		limit = 20
	}
	if offset > len(matched) {
		offset = len(matched)
	}
	end := offset + limit
	if end > len(matched) {
		end = len(matched)
	}
	hits := matched[offset:end]

	if len(query.Fields) > 0 {
		for i := range hits {
			hits[i].Payload = pickFields(hits[i].Payload, query.Fields)
		}
	}
	if lowerKeyword != "" && len(query.Highlight) > 0 {
		for i := range hits {
			for _, field := range query.Highlight {
				if raw, ok := hits[i].Payload[field]; ok {
					text := fmt.Sprintf("%v", raw)
					lower := strings.ToLower(text)
					if pos := strings.Index(lower, lowerKeyword); pos >= 0 {
						endPos := pos + len(lowerKeyword)
						hits[i].Payload[field] = text[:pos] + "<em>" + text[pos:endPos] + "</em>" + text[endPos:]
					}
				}
			}
		}
	}

	return search.Result{Total: total, Took: time.Since(start).Milliseconds(), Hits: hits, Facets: facets}, nil
}

func (c *fileConnection) Count(index string, query search.Query) (int64, error) {
	query.Offset = 0
	query.Limit = 0
	res, err := c.Search(index, query)
	if err != nil {
		return 0, err
	}
	return res.Total, nil
}

func (c *fileConnection) ensure(name string) (bleve.Index, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if idx, ok := c.indexes[name]; ok {
		return idx, nil
	}
	p := c.indexPath(name)
	if idx, err := bleve.Open(p); err == nil {
		c.indexes[name] = idx
		return idx, nil
	}

	mapping := buildIndexMapping(c.defs[name])
	mapping.DefaultField = "*"
	idx, err := bleve.New(p, mapping)
	if err != nil {
		return nil, err
	}
	c.indexes[name] = idx
	return idx, nil
}

func buildIndexMapping(index search.Index) *bmapping.IndexMappingImpl {
	mapping := bleve.NewIndexMapping()
	if len(index.Attributes) == 0 {
		return mapping
	}
	doc := bleve.NewDocumentMapping()
	for field, v := range index.Attributes {
		doc.AddFieldMappingsAt(field, buildFieldMapping(v))
	}
	mapping.DefaultMapping = doc
	return mapping
}

func buildFieldMapping(v Var) *bmapping.FieldMapping {
	t := strings.ToLower(strings.TrimSpace(v.Type))
	switch t {
	case "int", "int8", "int16", "int32", "int64",
		"uint", "uint8", "uint16", "uint32", "uint64",
		"float", "float32", "float64", "decimal",
		"number", "timestamp", "datetime", "date", "time":
		return bleve.NewNumericFieldMapping()
	case "bool", "boolean":
		return bleve.NewBooleanFieldMapping()
	default:
		f := bleve.NewTextFieldMapping()
		f.Store = true
		f.Index = true
		return f
	}
}

func (c *fileConnection) indexPath(name string) string {
	name = strings.TrimSpace(name)
	if c.prefix != "" {
		name = c.prefix + "_" + name
	}
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, " ", "_")
	if name == "" {
		name = "default"
	}
	return filepath.Join(c.root, name+".bleve")
}

func buildQuery(query search.Query) bquery.Query {
	parts := make([]bquery.Query, 0)
	if strings.TrimSpace(query.Keyword) != "" {
		if query.Prefix {
			parts = append(parts, bleve.NewPrefixQuery(strings.TrimSpace(query.Keyword)))
		} else {
			parts = append(parts, bleve.NewMatchQuery(query.Keyword))
		}
	}
	for _, f := range query.Filters {
		if q := filterQuery(f); q != nil {
			parts = append(parts, q)
		}
	}
	if len(parts) == 0 {
		return bleve.NewMatchAllQuery()
	}
	if len(parts) == 1 {
		return parts[0]
	}
	conj := bleve.NewConjunctionQuery(parts...)
	return conj
}

func filterQuery(f search.Filter) bquery.Query {
	op := strings.ToLower(strings.TrimSpace(f.Op))
	if op == "" {
		op = search.FilterEq
	}
	switch op {
	case search.FilterEq, "=":
		q := bleve.NewTermQuery(fmt.Sprintf("%v", f.Value))
		q.SetField(f.Field)
		return q
	case search.FilterIn:
		arr := f.Values
		if len(arr) == 0 && f.Value != nil {
			arr = []Any{f.Value}
		}
		queries := make([]bquery.Query, 0, len(arr))
		for _, one := range arr {
			q := bleve.NewTermQuery(fmt.Sprintf("%v", one))
			q.SetField(f.Field)
			queries = append(queries, q)
		}
		if len(queries) == 0 {
			return nil
		}
		return bleve.NewDisjunctionQuery(queries...)
	case search.FilterGt, ">", search.FilterGte, ">=", search.FilterLt, "<", search.FilterLte, "<=", search.FilterRange:
		var min, max *float64
		incMin := false
		incMax := false
		switch op {
		case search.FilterGt, ">":
			if v, ok := toFloat64(f.Value); ok {
				min = &v
			}
			incMin = false
		case search.FilterGte, ">=":
			if v, ok := toFloat64(f.Value); ok {
				min = &v
			}
			incMin = true
		case search.FilterLt, "<":
			if v, ok := toFloat64(f.Value); ok {
				max = &v
			}
			incMax = false
		case search.FilterLte, "<=":
			if v, ok := toFloat64(f.Value); ok {
				max = &v
			}
			incMax = true
		case search.FilterRange:
			if v, ok := toFloat64(f.Min); ok {
				min = &v
				incMin = true
			}
			if v, ok := toFloat64(f.Max); ok {
				max = &v
				incMax = true
			}
		}
		nq := bleve.NewNumericRangeInclusiveQuery(min, max, &incMin, &incMax)
		nq.SetField(f.Field)
		return nq
	default:
		q := bleve.NewTermQuery(fmt.Sprintf("%v", f.Value))
		q.SetField(f.Field)
		return q
	}
}

func toFloat64(v Any) (float64, bool) {
	switch vv := v.(type) {
	case float64:
		return vv, true
	case float32:
		return float64(vv), true
	case int:
		return float64(vv), true
	case int64:
		return float64(vv), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(vv), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func valueString(m Map, keys ...string) string {
	if m == nil {
		return ""
	}
	for _, key := range keys {
		if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func cloneMap(src Map) Map {
	if src == nil {
		return nil
	}
	out := Map{}
	for k, v := range src {
		out[k] = v
	}
	return out
}

func pickFields(payload Map, fields []string) Map {
	if payload == nil {
		return Map{}
	}
	if len(fields) == 0 {
		return cloneMap(payload)
	}
	out := Map{}
	for _, field := range fields {
		if v, ok := payload[field]; ok {
			out[field] = v
		}
	}
	return out
}

func compareForSort(a, b Any) int {
	if fa, oka := toFloat64(a); oka {
		if fb, okb := toFloat64(b); okb {
			switch {
			case fa < fb:
				return -1
			case fa > fb:
				return 1
			default:
				return 0
			}
		}
	}
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	switch {
	case sa < sb:
		return -1
	case sa > sb:
		return 1
	default:
		return 0
	}
}
