# Big Data Project - Vienna Willhaben Analysis

**GitHub:** https://github.com/Luatius/BigData-Project

> "Are flats near U-Bahn stations more expensive?" — **Yes, ~30% higher.**

## Key Finding

| Distance to U-Bahn | Avg. Price/m² |
|--------------------|---------------|
| <200m | 27.79 EUR |
| >1km | 21.17 EUR |

Correlation: r = -0.33 (Spearman, p < 0.001)

---

## Server Access

| Service | Access | Credentials |
|---------|--------|-------------|
| Jupyter | `http://<AZURE_IP>:8888` | `JUPYTER_PASSWORD` |
| Mongo Express | `http://<AZURE_IP>:8081` | `MONGO_EXPRESS_USER` / `MONGO_EXPRESS_PASSWORD` |
| SSH | `ssh <AZURE_USER>@<AZURE_IP>` | `AZURE_USER` / `AZURE_PASSWORD` |
| MongoDB | `mongodb://<USER>:<PASS>@<AZURE_IP>:27017` | See `.env` |

> Credentials in `.env` file (submitted separately).

---

## Quick Start (Local)

```bash
# Start services
docker compose up -d mongodb mongo-express jupyter

# Run scraper (Willhaben + U-Bahn data)
docker compose --profile scraper up scraper
```

- Jupyter: http://localhost:8888
- Mongo Express: http://localhost:8081

---

## Project Structure

```
├── notebooks/analysis.ipynb    # Full documentation + analysis
├── scripts/run_scraper.py      # Willhaben scraper
├── scripts/fetch_ubahn_stations.py  # Wiener Linien data
├── docker-compose.yml          # Local development
├── docker-compose.prod.yml     # Azure production
└── .github/workflows/deploy.yml  # CI/CD pipeline
```

---

## Documentation

**All detailed documentation is in the Jupyter notebook:**

[`notebooks/analysis.ipynb`](notebooks/analysis.ipynb)

Contents:
- Architecture diagram & components (Section 1)
- Data sources & structure (Section 2)
- Why MongoDB (Section 3)
- MapReduce implementation (Section 6)
- Big Data 5Vs analysis (Section 9)
- Visualizations & interactive map (Section 8)

---

## Tech Stack

MongoDB 8.2 · Python 3.11 · Docker · GitHub Actions · Azure VM
