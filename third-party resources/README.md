*Third-party documentation*

To minimize total repository size and respect potentially incongruous terms of 
redistribution, copies of documentation produced by third-parties are not
checked into this repository. However, all sources are/were available on the 
public internet. To retrieve third-party resources:

* Run the Python script `_fetch.py` with the source list file as its single
  argument.
      * Drag-n-drop `_documents.txt` onto `_fetch.py`
      * Launch via commmand line: `python _fetch.py _documents.txt`
* Manually visit the URLs listed in the plain-text document `_documents.txt`

The file `_document.txt` contains many lines of url/file name pairs. To be
parsed correctly: (1) only a single pair is allowed per line and (2) the file
name must follow the URL separated by *precisely* four spaces.
