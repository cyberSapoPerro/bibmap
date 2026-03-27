CREATE TABLE papers (
  doi TEXT PRIMARY KEY,
  title TEXT,
  publisher TEXT,
  container_title TEXT,
  reference_count INTEGER,
  is_referenced_by_count INTEGER,
  score INTEGER
);

CREATE TABLE citations (
  citing_doi TEXT,
  cited_doi TEXT,
  PRIMARY KEY (citing_doi, cited_doi),
  FOREIGN KEY (citing_doi) REFERENCES papers(doi) ON DELETE CASCADE,
  FOREIGN KEY (cited_doi) REFERENCES papers(doi) ON DELETE CASCADE,
  CHECK (citing_doi != cited_doi)
);

CREATE TABLE authors (
  given TEXT,
  family TEXT,
  PRIMARY KEY (given, family)
);

CREATE TABLE paper_author (
  paper_doi TEXT,
  author_given TEXT,
  author_family TEXT,
  sequence TEXT,
  PRIMARY KEY (paper_doi, author_given, author_family),
  FOREIGN KEY (paper_doi) REFERENCES papers(doi) ON DELETE CASCADE,
  FOREIGN KEY (author_given, author_family)
  REFERENCES authors(given, family)
  ON DELETE CASCADE
);

CREATE INDEX idx_citing ON citations(citing_doi);
CREATE INDEX idx_cited ON citations(cited_doi);
CREATE INDEX idx_pa_paper ON paper_author(paper_doi);
