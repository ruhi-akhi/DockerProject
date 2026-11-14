
import json
import math
import re
from pathlib import Path
from collections import Counter, defaultdict


class MiniSearchEngine:
    def __init__(self, folder="documents"):
        self.folder = Path(folder)
        self.index = defaultdict(dict)
        self.documents = {}

    def tokenize(self, text):
        return re.findall(r"\b[a-zA-Z0-9]+\b", text.lower())

    def create_sample_files(self):
        self.folder.mkdir(parents=True, exist_ok=True)

        samples = {
            "python.txt": (
                "Python is a powerful programming language. "
                "Python is useful for backend development."
            ),
            "javascript.txt": (
                "JavaScript is used for frontend development. "
                "Node.js is also useful for backend development."
            ),
            "database.txt": (
                "Database systems store and manage application data. "
                "PostgreSQL and MongoDB are popular databases."
            ),
            "async.txt": (
                "Async programming helps handle concurrent tasks. "
                "Python supports asynchronous programming."
            ),
        }

        for filename, content in samples.items():
            path = self.folder / filename

            if not path.exists():
                path.write_text(content, encoding="utf-8")

    def build_index(self):
        self.create_sample_files()

        self.index.clear()
        self.documents.clear()

        files = [
            path for path in self.folder.glob("*.txt")
            if path.is_file()
        ]

        for path in files:
            text = path.read_text(encoding="utf-8")
            words = self.tokenize(text)

            if not words:
                continue

            self.documents[path.name] = len(words)

            frequencies = Counter(words)

            for word, count in frequencies.items():
                self.index[word][path.name] = count

        print(f"Indexed {len(self.documents)} documents.")

    def search(self, query):
        query_words = self.tokenize(query)

        if not query_words:
            return []

        total_documents = len(self.documents)
        scores = defaultdict(float)

        for word in query_words:
            matching_docs = self.index.get(word, {})

            if not matching_docs:
                continue

            document_frequency = len(matching_docs)

            idf = math.log(
                1 + total_documents / document_frequency
            )

            for filename, frequency in matching_docs.items():
                total_words = self.documents[filename]

                tf = frequency / total_words

                scores[filename] += tf * idf

        results = sorted(
            scores.items(),
            key=lambda item: item[1],
            reverse=True,
        )

        return results

    def save_index(self, filename="search_index.json"):
        data = {
            "documents": self.documents,
            "index": {
                word: docs
                for word, docs in self.index.items()
            },
        }

        Path(filename).write_text(
            json.dumps(data, indent=4),
            encoding="utf-8",
        )

        print(f"Index saved to {filename}")

    def load_index(self, filename="search_index.json"):
        path = Path(filename)

        if not path.exists():
            raise FileNotFoundError(filename)

        data = json.loads(path.read_text(encoding="utf-8"))

        self.documents = data["documents"]
        self.index = defaultdict(
            dict,
            data["index"],
        )

        print("Search index loaded successfully.")

    def show_results(self, query):
        print(f"\nSearch query: {query}")

        results = self.search(query)

        if not results:
            print("No matching documents found.")
            return

        for rank, (filename, score) in enumerate(
            results,
            start=1,
        ):
            print(
                f"{rank}. {filename} "
                f"| Score: {score:.5f}"
            )


def main():
    engine = MiniSearchEngine()

    engine.build_index()

    engine.save_index()

    engine.show_results("python backend")
    engine.show_results("database")
    engine.show_results("async programming")


if __name__ == "__main__":
    main()
