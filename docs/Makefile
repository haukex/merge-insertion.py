
.PHONY: all installdeps clean

all: output/markdown/index.md

output/markdown/index.md: requirements.txt conf.py index.rst ../merge_insertion/__init__.py
	PYTHONPATH=.. sphinx-build -M markdown . output
	perl fixlinks.pl output/markdown/index.md

clean:
	rm -rf output
