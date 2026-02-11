"""Abstract base class for document-oriented data collectors.

For text-heavy, semi-structured data sources (news, speeches, articles, etc.)
where CSV would be inappropriate due to:
- Long text content with embedded quotes, commas, line breaks
- Nested/hierarchical metadata structures
- Variable schemas across documents
- Need for human-readable raw format

Uses JSONL (JSON Lines) format for Bronze layer:
- One JSON object per line
- Preserves document structure and metadata
- No escaping nightmares
- Streamable and appendable
- Industry standard for NLP/text corpora

Example Bronze output:
    data/raw/news/fed/statements_20260211.jsonl
    data/raw/news/reuters/articles_20260211.jsonl

Collectors are responsible ONLY for Bronze layer (raw data collection).
Preprocessing (Bronze → Silver) is handled by preprocessors.
"""

import json
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from src.shared.utils import setup_logger


class DocumentCollector(ABC):
    """Base class for document-oriented data collectors (news, speeches, articles).

    Use this for text-heavy sources where data is better represented as documents
    rather than tabular rows. Exports to JSONL format instead of CSV.

    Subclasses must define:
        SOURCE_NAME (str): identifier used in file naming (e.g. "fed", "reuters").

    Subclasses must implement:
        collect(): fetch all document collections for a date range.
        health_check(): verify the source is reachable.

    The export_jsonl() method handles Bronze JSONL naming automatically.

    Example:
        >>> from datetime import datetime
        >>> from pathlib import Path
        >>> from src.ingestion.collectors.document_collector import DocumentCollector
        >>>
        >>> class FedCollector(DocumentCollector):
        ...     SOURCE_NAME = "fed"
        ...
        ...     def collect(self, start_date, end_date):
        ...         return {
        ...             "statements": [
        ...                 {
        ...                     "timestamp_published": "2024-01-15T14:00:00Z",
        ...                     "title": "FOMC Statement",
        ...                     "content": "The Committee decided...",
        ...                     "document_type": "fomc_statement",
        ...                     "url": "https://...",
        ...                     "source": "fed"
        ...                 }
        ...             ],
        ...             "speeches": [...]
        ...         }
        ...
        ...     def health_check(self):
        ...         # Verify RSS feed is accessible
        ...         return True
        >>>
        >>> collector = FedCollector(output_dir=Path("data/raw/news/fed"))
        >>> data = collector.collect(start_date=datetime(2024, 1, 1))
        >>> for doc_type, documents in data.items():
        ...     path = collector.export_jsonl(documents, doc_type)
    """

    SOURCE_NAME: str  # e.g. "fed", "reuters", "ecb_speeches"

    def __init__(self, output_dir: Path, log_file: Path | None = None) -> None:
        """Initialize the document collector.

        Args:
            output_dir: Directory for raw JSONL exports (created if missing).
            log_file: Optional path for file-based logging.
        """
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = setup_logger(self.__class__.__name__, log_file)

    @abstractmethod
    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, list[dict]]:
        """Collect all document types from the source.

        Args:
            start_date: Start of the collection window.
            end_date: End of the collection window.

        Returns:
            Mapping of document type to list of document dictionaries.
            Each document dict should contain all source fields plus 'source'.

        Example return structure:
            {
                "statements": [
                    {
                        "timestamp_collected": "2024-01-15T12:00:00Z",
                        "timestamp_published": "2024-01-15T14:00:00Z",
                        "title": "...",
                        "content": "...",
                        "document_type": "fomc_statement",
                        "url": "...",
                        "source": "fed",
                        "metadata": {...}
                    },
                    ...
                ],
                "speeches": [...]
            }
        """
        ...

    @abstractmethod
    def health_check(self) -> bool:
        """Verify the data source is reachable and responding.

        Returns:
            True if the source is available, False otherwise.
        """
        ...

    def export_jsonl(
        self,
        documents: list[dict],
        document_type: str,
        collection_date: datetime | None = None,
    ) -> Path:
        """Export documents to JSONL following Bronze naming convention.

        File path: {output_dir}/{document_type}_{YYYYMMDD}.jsonl

        Each line in the output file is a complete JSON object representing
        one document. This format is:
        - Human-readable (can inspect with text editor)
        - Streamable (process line by line without loading all into memory)
        - Appendable (can add new documents without rewriting entire file)
        - Standard for NLP/text corpora

        Args:
            documents: List of document dictionaries to export.
            document_type: Document type identifier (e.g. "statements", "speeches").
            collection_date: Date to use in filename (default: today).

        Returns:
            Path to the written JSONL file.

        Raises:
            ValueError: If documents list is empty.

        Example:
            >>> documents = [
            ...     {"title": "Article 1", "content": "...", "source": "fed"},
            ...     {"title": "Article 2", "content": "...", "source": "fed"}
            ... ]
            >>> path = collector.export_jsonl(documents, "statements")
            >>> # Creates: data/raw/news/fed/statements_20260211.jsonl
        """
        if not documents:
            raise ValueError(f"Cannot export empty document list for '{document_type}'")

        date_str = (collection_date or datetime.now()).strftime("%Y%m%d")
        path = self.output_dir / f"{document_type}_{date_str}.jsonl"

        with open(path, "w", encoding="utf-8") as f:
            for doc in documents:
                # Write each document as a single JSON line
                # ensure_ascii=False preserves Unicode characters
                json.dump(doc, f, ensure_ascii=False)
                f.write("\n")

        self.logger.info("Exported %d documents to %s", len(documents), path)
        return path

    def export_all(
        self,
        data: dict[str, list[dict]] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, Path]:
        """Convenience method to collect and export all document types.

        If data is not provided, calls collect() first. Then exports each
        document type to a separate JSONL file.

        Args:
            data: Pre-collected data (if None, will call collect()).
            start_date: Start date for collection (only used if data is None).
            end_date: End date for collection (only used if data is None).

        Returns:
            Mapping of document type to exported file path.

        Example:
            >>> # Collect and export in one call
            >>> paths = collector.export_all(start_date=datetime(2024, 1, 1))
            >>> # {'statements': Path('...'), 'speeches': Path('...')}
            >>>
            >>> # Or export pre-collected data
            >>> data = collector.collect(start_date=datetime(2024, 1, 1))
            >>> paths = collector.export_all(data=data)
        """
        if data is None:
            self.logger.info("No data provided, collecting from source...")
            data = self.collect(start_date=start_date, end_date=end_date)

        paths = {}
        for doc_type, documents in data.items():
            if documents:  # Only export if we have documents
                paths[doc_type] = self.export_jsonl(documents, doc_type)
            else:
                self.logger.warning("No documents for type '%s', skipping export", doc_type)

        self.logger.info("Exported %d document types", len(paths))
        return paths
