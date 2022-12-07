from collections import defaultdict
import itertools
from pathlib import Path
from datetime import datetime
import shutil

EDGES_FILENAME = "cit-HepPh.txt"
NODES_DATES_FILENAME = "cit-HepPh-dates.txt"
BATCH_DIR = "spark"

class Partitioner:
    def __init__(self, edgesPath: str, node_date_path: str) -> None:
        edgesPath = Path(edgesPath)
        node_date_path = Path(node_date_path)
        self.src_dst = Partitioner.parse_edges(edgesPath)
        self.node_dates, self.node_to_dates = Partitioner.parse_node_dates(
            node_date_path
        )
        self.years = sorted(set(date.year for _, date in self.node_dates))

    def spark(self, path: str = None) -> None:
        path = Path(path) if path else Path.cwd() / BATCH_DIR
        shutil.rmtree(path.absolute().as_posix(), ignore_errors=True)
        path.mkdir(parents=True, exist_ok=True)
        for year in self.years:
            curr_year_verts_path = path / f"{year}-verts.txt"
            curr_year_edges_path = path / f"{year}-edges.txt"
            with curr_year_verts_path.open(mode="w") as v, \
                curr_year_edges_path.open(mode="w") as e:
                verts = set()
                for src, date in self.node_dates:
                    if date.year >= year + 1:
                        continue
                    if src not in self.src_dst:
                        src = int(str(src).removeprefix("11"))
                        if src not in self.src_dst:
                            continue
                    verts.add(src)
                    for dst in self.src_dst[src]:
                        verts.add(dst)
                        e.write(f"{src} {dst}\n")
                for vert in verts:
                    v.write(f"{vert}\n")
                    
    @staticmethod
    def from_default(path: str = None) -> "Partitioner":
        path = Path(path) if path else Path.cwd()
        node_date_path = (path / NODES_DATES_FILENAME).absolute().as_posix()
        edgesPath = (path / EDGES_FILENAME).absolute().as_posix()
        return Partitioner(edgesPath, node_date_path)

    @staticmethod
    def parse_edges(path: Path) -> dict[str, list[str]]:
        src_dst = defaultdict(list)
        nodes = set()
        with path.open(mode="r") as f:
            for line in f:
                line = line.strip()
                if line.startswith("#") or line == "":
                    continue
                src, dst = line.split()
                src, dst = int(src), int(dst)
                src_dst[src].append(dst)
                nodes.add(src)
                nodes.add(dst)
        for node in nodes:
            if node not in src_dst:
                src_dst[node] = []
        return src_dst

    @staticmethod
    def parse_node_dates(path: Path) -> list[tuple[str, datetime]]:
        node_dates = []
        node_to_dates = dict()
        with path.open(mode="r") as f:
            for line in f:
                line = line.strip()
                if line.startswith("#") or line == "":
                    continue
                node, date = line.split()
                node = int(node)
                date = datetime.strptime(date, "%Y-%m-%d")
                node_dates.append((node, date))
                node_to_dates[node] = date
        # sort by date
        node_dates.sort(key=lambda x: x[1])
        return node_dates, node_to_dates


if __name__ == "__main__":
    partitioner = Partitioner.from_default()
    partitioner.spark()
