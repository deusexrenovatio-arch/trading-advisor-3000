# Architecture Map

```mermaid
flowchart LR
  A["Governance Contract"] --> B["Process Runtime"]
  B --> C["Durable State"]
  B --> D["Skills Governance"]
  C --> E["Reporting"]
  B --> F["Placeholder App"]
  F --> G["Process + Architecture Tests"]
  G --> H["CI Lanes"]
```
