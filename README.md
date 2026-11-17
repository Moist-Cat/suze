# Suze (WIP)

All-in-one data orchestrator. Build I/O bound pipelines that are safe to run twice.

A pieline is a request that might depend on one or more requests; each request loads data from a source, transforms the data in-memory, and returns validated data.

Example:

```python
# in-memory source
# groups many requests together
dmux = Demultiplexer("dmux")

master = DataRequest(
    source=dmux,
    request_id="master",
    stages={
        "load": {"parameters": {"requests": all_page_requests}},
        "combine": {"strategy": merge_batches},
        # compute insight tags before any filtering
        "add_percentiles": {"strategy": lambda records, _: add_percentiles(records)},
        "compute_insight_tags": {
            "strategy": lambda records, _: compute_insight_tags(records, _)
        },
        "feature_extract": {
            "strategy": lambda records, _: [
                {
                    "remote_id": r["remote_id"],
                    "score": r["score"],
                    "scored_by": r["scored_by"],
                    "full_record": r,  # r now includes "insight_tags"
                }
                for r in records
            ]
        },
        "cluster": {"strategy": lambda records, _: _apply_kmeans(records)},
        "filter_best_cluster": {
            "strategy": lambda records, _: _select_best_cluster(records)
        },
        "tag_partitions": { "strategy": lambda records, _: detect_tag_partitions(records)},
        "convert": {"strategy": lambda records, _: convert_to_movie_metadata(records)},
        "merge": {"strategy": lambda records, _: merge_with_existing(records)},
        "add_timestamps": {"strategy": lambda records, _: add_metadata_fields(records)},
        "final_validate": {"strategy": lambda records, _: final_validate(records)},
    },
    dependency_requirements=[...]
)
```
