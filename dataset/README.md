# DataSet
Get and parse dataset from https://snap.stanford.edu/data/cit-HepPh.html
For timely dataflow and spark

## Download datafile 
```bash
curl -fsSL "https://snap.stanford.edu/data/cit-HepPh.txt.gz" | gunzip -d > cit-HepPh.txt
curl -fsSL "https://snap.stanford.edu/data/cit-HepPh-dates.txt.gz" | gunzip -d > cit-HepPh-dates.txt
```

## Set up Python virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
```

## Run data parser
```bash
python parser.py
```
data parsed for spark will be created under ./spark