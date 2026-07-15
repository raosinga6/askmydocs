# Embedding Input Contract

## Output schema (one row per table)

| column | type | example |
|---|---|---|
| table_name | string | action_after_ticket_closure_base |
| business_domain | string | support (inferred from upstream namespaces) |
| text_blob | string | (see template below) |
| metadata | struct | (see below) |

### text_blob template

A single string composed of 5 labeled sections separated by `\n\n`. The labels
help the embedding model attend to different aspects of the table: