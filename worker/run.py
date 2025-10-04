"""
Legacy entry point for backward compatibility.

This module provides a simple entry point that uses the new
adapter-based architecture while maintaining backward compatibility.
"""

from .service import main

if __name__ == "__main__":
    main()
