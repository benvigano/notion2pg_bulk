"""
Progress tracking utilities for migration operations.
"""

from typing import Optional
from tqdm import tqdm


class ProgressTracker:
    """Manages progress bars for different migration phases."""
    
    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self._current_bar: Optional[tqdm] = None
    
    def start_phase(self, description: str, total: Optional[int] = None) -> None:
        """Start a new progress phase."""
        if not self.verbose:
            return
            
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
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]"
            )
    
    def update(self, n: int = 1) -> None:
        """Update current progress bar."""
        if self.verbose and self._current_bar is not None:
            self._current_bar.update(n)
    
    def set_postfix(self, **kwargs) -> None:
        """Set postfix information on current progress bar."""
        if self.verbose and self._current_bar is not None:
            self._current_bar.set_postfix(**kwargs)
    
    def finish_phase(self) -> None:
        """Finish current progress phase."""
        if self.verbose and self._current_bar is not None:
            self._current_bar.close()
            self._current_bar = None
    
    def log(self, message: str) -> None:
        """Log a message, ensuring it doesn't interfere with progress bars."""
        if not self.verbose:
            return
            
        if self._current_bar is not None:
            tqdm.write(message)
        else:
            print(message)
    
    def cleanup(self) -> None:
        """Clean up any remaining progress bars."""
        if self._current_bar is not None:
            self._current_bar.close()
            self._current_bar = None


