# MinerU annual-report trial

MinerU 4.0.12 `basic` was tested locally with CPU ONNX models against pages 3 and 4 of a 249-page Chinese annual report. Page 4 was also rendered at 200 DPI and processed as an image, to exercise OCR rather than relying on a native PDF text layer. The image test produced 13 retained regions including five tables; the first heading and company name matched the source image. The first table retained HTML including a merged cell. These are spot checks, not a full accuracy or financial-table benchmark; formula, scan quality, layout ordering, and GPU execution remain unvalidated.

The adapter accepts only MinerU 4.0.12 `docvortex.middle` 2.0, converts normalized boxes to PDF coordinates, retains table HTML, and does not invent OCR confidence or cell coordinates. It uses rendered page images and OCR, including region clips for mixed pages. MinerU is the default for the Ray pipeline.

The default settings are `PDF_LAYOUT_ENGINE=mineru`, `PDF_MINERU_TIER=basic`, and `PDF_PARSER_VERSION=annual-report-mineru-basic-4.0.12-v1`. Install `mineru==4.0.12` on the Ray execution node; the backend passes these settings to Ray jobs. Explicit environment settings override the defaults. Tier or model changes require a new parser version to avoid reusing incompatible checkpoints. The Ray pipeline supports both MinerU and Paddle parsing engines.

MinerU basic ONNX execution observed here used CPU; a Ray GPU reservation does not imply GPU inference. A 143-page 2024 annual report was parsed, written to Silver, indexed, and retrieved successfully. This end-to-end check is not a full accuracy benchmark.
