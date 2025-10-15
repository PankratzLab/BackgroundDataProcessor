## Background Data Processor  
### A lightweight Java utility for single-pass processing of item collections. 

    
Designed to improve application responsiveness with the following key features:

**Hybrid Loading**: The constructor blocks until the _first_ item in a collection is loaded; then processes the remaining items _asynchronously_ in the background. This allows your application to start working with data immediately.

**Thread-Safety**: Guarantees safe access in concurrent environments. Calls to retrieve items that are still being processed _will block_ until the data is ready.

**Memory-Efficiency**: Operates in a _single-pass_ workflow. Once an item is retrieved, its reference is _dropped_ from the internal cache to minimize memory footprint. Retrieving the same item again will trigger a new, synchronous load. If items require multiple uses, references to the items _must_ be maintained externally.
