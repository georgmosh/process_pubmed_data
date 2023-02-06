
# Data processing for PubMed baseline dump

### Details
This is a set of scripts to download `tar.gz` files from PubMed, transform them into `json` format and index them into an Elasticsearch engine.

For Pubmed indexing we used ElasticSearch v5.0.1 with Lucene v6.2.1 

### Steps:

###### Download and verify data

```
python detect_and_download_new.py
```  

###### Create ElasticSearch index

```
python create_index.py
```  

###### Index data to the new index

```
python upload_pubmed_to_elastic.py
```  
 


### Cite
If you find our work useful please cite our paper:

```
@inproceedings{pappas-androutsopoulos-2021-neural,
    title = "A Neural Model for Joint Document and Snippet Ranking in Question Answering for Large Document Collections",
    author = "Pappas, Dimitris  and
      Androutsopoulos, Ion",
    booktitle = "Proceedings of the 59th Annual Meeting of the Association for Computational Linguistics and the 11th International Joint Conference on Natural Language Processing (Volume 1: Long Papers)",
    month = aug,
    year = "2021",
    address = "Online",
    publisher = "Association for Computational Linguistics",
    url = "https://aclanthology.org/2021.acl-long.301",
    doi = "10.18653/v1/2021.acl-long.301",
    pages = "3896--3907"
}
```


