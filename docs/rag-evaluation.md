# RAG 评测集标注说明

评测集必须来自已进入 Silver 的新闻文章，而不是通用问答数据。每条样本应包含真实中文金融问题、正确文章的 `news_id`、支持结论的 `chunk_id`、问题类型和时间范围。

首批至少标注 100 条，按个股事件、财报、宏观、行业主题、精确关键词、同义表达、时效问题和无答案问题分层抽样。`tests/rag/evaluation_samples.json` 仅是格式模板，不含人工确认的 `expected_article_ids` 和 `expected_chunk_ids` 时不得用于计算 Recall@K、MRR 或 nDCG。

每次检索策略或模型升级都必须在固定标注集上回归；记录 Recall@50、MRR、nDCG@5、文章重复率、阈值拒答准确率和引用完整率。
