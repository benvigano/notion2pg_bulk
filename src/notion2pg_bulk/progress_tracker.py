"""
Progress tracking utilities for migration operations.
"""

from typing import Optional
from tqdm import tqdm


class ProgressTracker:
    """Manages progress bars for different migration phases."""
    
    def __init__(self, interactive_mode: bool = True):
        self.interactive_mode = interactive_mode
        self._current_bar: Optional[tqdm] = None
        self._detail_bar: Optional[tqdm] = None
    
    def start_phase(self, description: str, total: Optional[int] = None) -> None:
        """Start a new progress phase."""
        if not self.interactive_mode:
            return

        if self._detail_bar is not None:
            self._detail_bar.close()
            self._detail_bar = None

        if self._current_bar is not None:
            self._current_bar.close()
        
        if total is None:
            # For indeterminate progress, use a simpler format
            self._current_bar = tqdm(
                desc=description,
                unit="requests",
                bar_format="{l_bar}{n_fmt} requests [{elapsed}]"
            )
        else:
            self._current_bar = tqdm(
                total=total,
                desc=description,
                unit="items",
                bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt} [{elapsed}<{remaining}]"
            )
    
    def update(self, n: int = 1) -> None:
        """Update current progress bar."""
        if self.interactive_mode and self._current_bar is not None:
            self._current_bar.update(n)

    def start_detail(self, description: str, unit: str = "items") -> None:
        """Start a nested indeterminate progress indicator."""
        if not self.interactive_mode:
            return

        if self._detail_bar is not None:
            self._detail_bar.close()

        self._detail_bar = tqdm(
            desc=description,
            unit=unit,
            position=1,
            leave=False,
            bar_format="{l_bar}{n_fmt} {unit} [{elapsed}]",
        )

    def update_detail(self, n: int = 1) -> None:
        """Update the nested progress indicator."""
        if self.interactive_mode and self._detail_bar is not None:
            self._detail_bar.update(n)

    def set_detail_postfix(self, **kwargs) -> None:
        """Set postfix information on the nested progress indicator."""
        if self.interactive_mode and self._detail_bar is not None:
            self._detail_bar.set_postfix(**kwargs)

    def finish_detail(self) -> None:
        """Finish the nested progress indicator."""
        if self.interactive_mode and self._detail_bar is not None:
            self._detail_bar.close()
            self._detail_bar = None
    
    def set_postfix(self, **kwargs) -> None:
        """Set postfix information on current progress bar."""
        if self.interactive_mode and self._current_bar is not None:
            self._current_bar.set_postfix(**kwargs)
    
    def finish_phase(self) -> None:
        """Finish current progress phase."""
        if self.interactive_mode and self._detail_bar is not None:
            self._detail_bar.close()
            self._detail_bar = None

        if self.interactive_mode and self._current_bar is not None:
            self._current_bar.close()
            self._current_bar = None
    
    def log(self, message: str) -> None:
        """Log a message, ensuring it doesn't interfere with progress bars."""
        if not self.interactive_mode:
            return
            
        if self._current_bar is not None:
            tqdm.write(message)
        else:
            print(message)
    
    def cleanup(self) -> None:
        """Clean up any remaining progress bars."""
        if self._detail_bar is not None:
            self._detail_bar.close()
            self._detail_bar = None

        if self._current_bar is not None:
            self._current_bar.close()
            self._current_bar = None


