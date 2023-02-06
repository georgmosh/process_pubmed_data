
from elasticsearch import Elasticsearch

index   = 'pubmed_abstracts_0_1'
map     = 'abstract_map_0_1'

elastic_con = Elasticsearch(
	['localhost:9200'],
    verify_certs=True,
    timeout=150,
    max_retries=10,
    retry_on_timeout=True
)

elastic_con.indices.delete(
    index=index,
    ignore=[400,404]
)

mapping = {
    "mappings":{
        map:{
            "properties":{
                ######### TEXT
                'ArticleTitle'      : {
                    "type"      : "text",
                    "analyzer"  : "english",
                },
                'VernacularTitle'   : {
                    "type"      : "text",
                    "analyzer"  : "english",
                },
                'AbstractText'      	: { "type" : "text", "analyzer"  : "english",},
                ######### KEYWORDS
                'Language'          : {
                    "type": "keyword"
                },
                'pmid'              : {
                    "type": "keyword"
                },
                'CitationSubset'    : {
                    "type": "keyword"
                },
                ######### DATES
                'ArticleDate'       : {
                    "type"      : "date",
                    "format"    : "yyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||yyyy/MM/dd||dd/MM/yyyy||epoch_millis||EE MMM d HH:mm:ss Z yyyy"
                },
                'DateCreated'     	: {
                    "type"      : "date",
                    "format"    : "yyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||yyyy/MM/dd||dd/MM/yyyy||epoch_millis||EE MMM d HH:mm:ss Z yyyy"
                },
                'DateCompleted'     : {
                    "type"      : "date",
                    "format"    : "yyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||yyyy/MM/dd||dd/MM/yyyy||epoch_millis||EE MMM d HH:mm:ss Z yyyy"
                },
                'DateRevised'       : {
                    "type"      : "date",
                    "format"    : "yyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||yyyy/MM/dd||dd/MM/yyyy||epoch_millis||EE MMM d HH:mm:ss Z yyyy"
                },
                ######### NESTED
				'Authors'			 	: {
					"type": "nested",
					"properties": {
						"AffiliationInfo": {
							"type": "text",
							"analyzer": "english",
                            "fields": {"raw": {"type": "keyword"}}
						},
						"CollectiveName": {
							"type": "text",
							"analyzer": "english",
                            "fields": {"raw": {"type": "keyword"}}
						},
						"ForeName": {
							"type": "text",
							"analyzer": "english",
                            "fields": {"raw": {"type": "keyword"}}
						},
						"Initials": {
							"type": "text",
							"analyzer": "english",
                            "fields": {"raw": {"type": "keyword"}}
						},
						"LastName": {
							"type": "text",
							"analyzer": "english",
                            "fields": {"raw": {"type": "keyword"}}
						}
					}
				},
				'Chemicals'         	: {
                    "properties": {
                        "UI": {
                            "type": "keyword"
                        },
                        "NameOfSubstance": {
                            "type"      : "text",
                            "analyzer"  : "english",
                            "fields": {
                                "raw": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "RegistryNumber": {
                            "type": "keyword",
                        },
                    }
                },
                'OtherIDs'          	: {
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "Source": {
                            "type": "keyword"
                        },
                    }
                },
                'AbstractSegments'  	: {
                    "properties": {
                        'NlmCategory' : {
                            "type"      : "text",
                            "analyzer"  : "english",
                            "fields": {
                                "raw": {
                                    "type": "keyword"
                                }
                            }
                        },
                        'Label' : {
                            "type"      : "text",
                            "analyzer"  : "english",
                            "fields": {
                                "raw": {
                                    "type": "keyword"
                                }
                            }
                        },
                        'text' : {
                            "type"      : "text",
                            "analyzer"  : "english"
                        },
                    }
                },
                'MeshHeadings'      	: {
                    "properties": {
                        'UI' : {"type": "keyword"},
                        'MajorTopicYN' : {"type": "keyword"},
                        'Type' : {"type": "keyword"},
                        'text' : {
                            "type"      : "text",
                            "analyzer"  : "english",
                            "fields": {"raw": {"type": "keyword"}}
                        },
                        'Label' : {
                            "type"      : "text",
                            "analyzer"  : "english",
                            "fields": {"raw": {"type": "keyword"}}
                        },
                    }
                },
                'Grants'            	: {
                    "properties": {
                        'Country' : {
                            "type": "keyword"
                        },
                        'GrantID' : {
                            "type": "keyword"
                        },
                        'Agency' : {
                            "type": "keyword"
                        },
                    }
                },
                "Keywords"          	: {
                    "type"      : "text",
                    "analyzer"  : "english",
                    "fields": {
                        "raw": {
                            "type": "keyword"
                        }
                    }
                },
                'SupplMeshList'     	: {
                    "properties": {
                        'text' : {
                            "type"      : "text",
                            "analyzer"  : "english",
                            "fields": {
                                "raw": {
                                    "type": "keyword"
                                }
                            }
                        },
                        'GrantID' : {
                            "type": "keyword"
                        },
                        'Agency' : {
                            "type": "keyword"
                        },
                    }
                },
                'Journal'           	: {
                    "properties": {
                        'Title' : {
                            "type"      : "text",
                            "analyzer"  : "english",
                            "fields": {
                                "raw": {
                                    "type": "keyword"
                                }
                            }
                        },
                        'ISOAbbreviation' : {
                            "type": "keyword"
                        },
                        'ISSN' : {
                            "type": "keyword"
                        },
						'Pagination' : {
                            "type": "keyword"
                        },
                        'JournalIssue' : {
                            "properties": {
                                'Volume' : {"type": "keyword"},
                                'Issue' : {"type": "keyword"},
                                'PubDate' : {"type": "keyword"},
                            }
                        },
                    }
                },
                'PublicationType'   	: {
                    "properties": {
                        'Type': {
                            "type": "text",
                            "analyzer": "english",
                            "fields": {"raw": {"type": "keyword"}}
                        },
                        'UI': {
                            "type": "keyword"
                        }
                    }
                },
                'ELocationIDs'      	: {
                    "properties": {
                        'value': {
                            "type": "keyword"
                        },
                        'ValidYN': {
                            "type": "keyword"
                        },
                        'EIdType': {
                            "type": "keyword"
                        }
                    }
                },
                'DataBankList'      	: {
                    "properties": {
                        'DataBankName': {
                            "type": "text",
                            "analyzer": "english",
                            "fields": {
                                "raw": {
                                    "type": "keyword"
                                }
                            }
                        },
                        'AccessionNumbers': {
                            "type": "keyword"
                        }
                    }
                },
				'AbstractLabel'			: {"type": "text", "analyzer": "english", "fields": {"raw": {"type": "keyword"}}},
				'AbstractNlmCategor'	: {"type": "text", "analyzer": "english", "fields": {"raw": {"type": "keyword"}}},
				"AbstractLanguage"		: {"type": "text", "analyzer": "english", "fields": {"raw": {"type": "keyword"}}},
				"AbstractType"			: {"type": "text", "analyzer": "english", "fields": {"raw": {"type": "keyword"}}},
				"MedlineJournalInfo"	: {
					"type": "nested",
					"properties": {
						"Country": {"type": "keyword"},
						"ISSNLinking": {"type": "keyword"},
						"MedlineTA": {"type": "keyword"},
						"NlmUniqueID": {"type": "keyword"},
					}
				},
				"references"			: {
					"type": "nested",
					"properties": {
						"Note": {"type": "text", "analyzer": "english", "fields": {"raw": {"type": "keyword"}}},
						"RefSource": {"type": "text", "analyzer": "english", "fields": {"raw": {"type": "keyword"}}},
						"PMID": {"type": "keyword"},
						"RefType": {"type": "keyword"},
					}
				},

            }
        }
    }
}

elastic_con.indices.create(index=index, ignore=400, body=mapping)


