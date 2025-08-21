# Mira
# Mira v0

**Mira v0** is a specialized Large Language Model (LLM) designed to assist researchers by providing answers **based solely on curated content**. Unlike typical models that draw from the entire web, Mira is **context-aware and scoped** — it only responds using a carefully selected knowledge base.

A key feature of Mira is its ability to **recognize when it does not have an answer**, redirecting users to the internal Molecule team for further assistance. This ensures both **accuracy and reliability**.

---

## About This Repository

This repository contains the automated crawler infrastructure that powers Mira v0's knowledge base. It includes:

- Web crawler script that fetches content from curated sources
- Embedding generation logic to process and vectorize the collected documents
- Database management for storing and organizing the processed content
- GitHub Actions workflow that runs the crawler on a daily schedule

The crawler automatically collects and processes public documents from sources curated and maintained by the Molecule team, transforming them into the contextual knowledge base that enables Mira to provide precise and relevant responses.

---

## Main Sources

The crawler collects and processes content from the following curated sources:

- [Molecule Documentation](https://molecule.to/)
- [Molecule Blogs](https://molecule.to/blog)
- [DeSci Codes Documentation](https://docs.descicodes.com/)

---

## Live Version

You can try the live version of Mira v0 here:  
[https://mira.molecule.xyz/](https://mira.molecule.xyz/)

---

## Contact

For inquiries or support, please reach out to:

**Email:** [maria@molecule.to](mailto:maria@molecule.to)
