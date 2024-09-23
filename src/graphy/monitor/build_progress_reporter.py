from graphrag.index.progress import ProgressReporter
from datashaper.progress import Progress
from tqdm import tqdm

class BuildProgressReporter(ProgressReporter):
    """A progress reporter that displays a progress bar on the command-line."""

    prefix: str
    last_description: str

    def __init__(self, prefix: str):
        """Create a new progress reporter."""
        self.prefix = prefix
        self.last_description = ""
        self.pbar = tqdm(total=100)

        print(f"\n{self.prefix}", end="")  # noqa T201

    def __call__(self, update: Progress) -> None:
        """Update progress."""
        perc = update.percent
        if perc is None:
            if update.completed_items is None or update.total_items is None or update.total_items == 0:
                perc = 0
            else:
                perc = (update.completed_items * 1.0) / (update.total_items * 1.0)

        if update.description is not None and update.description != self.last_description:
            self.pbar.write(f"{update.description}")
            self.last_description = update.description

        self.pbar.n = perc*100.0
        self.pbar.update(0)
        # print(".", end="")  # noqa T201

    def dispose(self) -> None:
        """Dispose of the progress reporter."""
        pass

    def child(self, prefix: str, transient: bool = True) -> "ProgressReporter":
        """Create a child progress bar."""
        return BuildProgressReporter(prefix)

    def stop(self) -> None:
        """Stop the progress reporter."""
        pass

    def force_refresh(self) -> None:
        """Force a refresh."""
        pass

    def error(self, message: str) -> None:
        """Report an error."""
        print(f"\n{self.prefix}ERROR: {message}")  # noqa T201

    def warning(self, message: str) -> None:
        """Report a warning."""
        print(f"\n{self.prefix}WARNING: {message}")  # noqa T201

    def info(self, message: str) -> None:
        """Report information."""
        print(f"\n{self.prefix}INFO: {message}")  # noqa T201

    def success(self, message: str) -> None:
        """Report success."""
        print(f"\n{self.prefix}SUCCESS: {message}")  # noqa T201
