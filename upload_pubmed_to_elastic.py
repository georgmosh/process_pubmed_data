#!/usr/bin/env python
# -*- coding: utf-8 -*-

#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- encoding: utf-8 -*-

import os, json, gzip, traceback
from lxml import etree
from pprint import pprint
from tqdm import tqdm
from dateutil import parser
from zipfile import ZipFile
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import sys

html_special = {
 '&AElig;'  : 'Æ',
 '&Aacute;' : 'Á',
 '&Acirc;'  : 'Â',
 '&Agrave;' : 'À',
 '&Alpha;'  : 'Α',
 '&Aring;'  : 'Å',
 '&Atilde;' : 'Ã',
 '&Auml;'   : 'Ä',
 '&Beta;'   : 'Β',
 '&Ccedil;' : 'Ç',
 '&Chi;'    : 'Χ',
 '&Dagger;' : '‡',
 '&Delta;'  : 'Δ',
 '&ETH;'    : 'Ð',
 '&Eacute;' : 'É',
 '&Ecirc;'  : 'Ê',
 '&Egrave;' : 'È',
 '&Epsilon;': 'Ε',
 '&Eta;'    : 'Η',
 '&Euml;'   : 'Ë',
 '&Gamma;'  : 'Γ',
 '&Iacute;' : 'Í',
 '&Icirc;'  : 'Î',
 '&Igrave;' : 'Ì',
 '&Iota;'   : 'Ι',
 '&Iuml;'   : 'Ï',
 '&Kappa;'  : 'Κ',
 '&Lambda;' : 'Λ',
 '&Mu;'     : 'Μ',
 '&Ntilde;' : 'Ñ',
 '&Nu;'     : 'Ν',
 '&OElig;'  : 'Œ',
 '&Oacute;' : 'Ó',
 '&Ocirc;'  : 'Ô',
 '&Ograve;' : 'Ò',
 '&Omega;'  : 'Ω',
 '&Omicron;': 'Ο',
 '&Oslash;' : 'Ø',
 '&Otilde;' : 'Õ',
 '&Ouml;'   : 'Ö',
 '&Phi;'    : 'Φ',
 '&Pi;'     : 'Π',
 '&Prime;'  : '″',
 '&Psi;'    : 'Ψ',
 '&Rho;'    : 'Ρ',
 '&Scaron;' : 'Š',
 '&Sigma;'  : 'Σ',
 '&THORN;'  : 'Þ',
 '&Tau;'    : 'Τ',
 '&Theta;'  : 'Θ',
 '&Uacute;' : 'Ú',
 '&Ucirc;'  : 'Û',
 '&Ugrave;' : 'Ù',
 '&Upsilon;': 'Υ',
 '&Uuml;'   : 'Ü',
 '&Xi;'     : 'Ξ',
 '&Yacute;' : 'Ý',
 '&Yuml;'   : 'Ÿ',
 '&Zeta;'   : 'Ζ',
 '&aacute;' : 'á',
 '&acirc;'  : 'â',
 '&acute;'  : '´',
 '&aelig;'  : 'æ',
 '&agrave;' : 'à',
 '&alefsym;': 'ℵ',
 '&alpha;'  : 'α',
 '&amp;'    : '&',
 '&and;'    : '⊥',
 '&ang;'    : '∠',
 '&apos;'   : '&apos;',
 '&aring;'  : 'å',
 '&asymp;'  : '≈',
 '&atilde;' : 'ã',
 '&auml;'   : 'ä',
 '&bdquo;'  : '„',
 '&beta;'   : 'β',
 '&brvbar;' : '¦',
 '&bull;'   : '•',
 '&cap;'    : '∩',
 '&ccedil;' : 'ç',
 '&cedil;'  : '¸',
 '&cent;'   : '¢',
 '&chi;'    : 'χ',
 '&circ;'   : 'ˆ',
 '&clubs;'  : '♣',
 '&cong;'   : '≅',
 '&copy;'   : '©',
 '&crarr;'  : '↵',
 '&cup;'    : '∪',
 '&curren;' : '¤',
 '&dArr;'   : '⇓',
 '&dagger;' : '†',
 '&darr;'   : '↓',
 '&deg;'    : '°',
 '&delta;'  : 'δ',
 '&diams;'  : '♦',
 '&divide;' : '÷',
 '&eacute;' : 'é',
 '&ecirc;'  : 'ê',
 '&egrave;' : 'è',
 '&empty;'  : '∅',
 '&emsp;'   : 'emsp',
 '&ensp;'   : 'ensp',
 '&epsilon;': 'ε',
 '&equiv;'  : '≡',
 '&eta;'    : 'η',
 '&eth;'    : 'ð',
 '&euml;'   : 'ë',
 '&euro;'   : '€',
 '&exist;'  : '∃',
 '&fnof;'   : 'ƒ',
 '&forall;' : '∀',
 '&frac12;' : '½',
 '&frac14;' : '¼',
 '&frac34;' : '¾',
 '&frasl;'  : '⁄',
 '&gamma;'  : 'γ',
 '&ge;'     : '≥',
 '&gt;'     : '>',
 '&hArr;'   : '⇔',
 '&harr;'   : '↔',
 '&hearts;' : '♥',
 '&hellip;' : '…',
 '&iacute;' : 'í',
 '&icirc;'  : 'î',
 '&iexcl;'  : '¡',
 '&igrave;' : 'ì',
 '&image;'  : 'ℑ',
 '&infin;'  : '∞',
 '&int;'    : '∫',
 '&iota;'   : 'ι',
 '&iquest;' : '¿',
 '&isin;'   : '∈',
 '&iuml;'   : 'ï',
 '&kappa;'  : 'κ',
 '&lArr;'   : '⇐',
 '&lambda;' : 'λ',
 '&lang;'   : '〈',
 '&laquo;'  : '«',
 '&larr;'   : '←',
 '&lceil;'  : '⌈',
 '&ldquo;'  : '“',
 '&le;'     : '≤',
 '&lfloor;' : '⌊',
 '&lowast;' : '∗',
 '&loz;'    : '◊',
 '&lrm;'    : 'lrm',
 '&lsaquo;' : '‹',
 '&lsquo;'  : '‘',
 '&lt;'     : '<',
 '&macr;'   : '¯',
 '&mdash;'  : '—',
 '&micro;'  : 'µ',
 '&middot;' : '·',
 '&minus;'  : '−',
 '&mu;'     : 'μ',
 '&nabla;'  : '∇',
 '&nbsp;'   : 'nbsp',
 '&ndash;'  : '–',
 '&ne;'     : '≠',
 '&ni;'     : '∋',
 '&not;'    : '¬',
 '&notin;'  : '∉',
 '&nsub;'   : '⊄',
 '&ntilde;' : 'ñ',
 '&nu;'     : 'ν',
 '&oacute;' : 'ó',
 '&ocirc;'  : 'ô',
 '&oelig;'  : 'œ',
 '&ograve;' : 'ò',
 '&oline;'  : '‾',
 '&omega;'  : 'ω',
 '&omicron;': 'ο',
 '&oplus;'  : '⊕',
 '&or;'     : '⊦',
 '&ordf;'   : 'ª',
 '&ordm;'   : 'º',
 '&oslash;' : 'ø',
 '&otilde;' : 'õ',
 '&otimes;' : '⊗',
 '&ouml;'   : 'ö',
 '&para;'   : '¶',
 '&part;'   : '∂',
 '&permil;' : '‰',
 '&perp;'   : '⊥',
 '&phi;'    : 'φ',
 '&pi;'     : 'π',
 '&piv;'    : 'ϖ',
 '&plusmn;' : '±',
 '&pound;'  : '£',
 '&prime;'  : '′',
 '&prod;'   : '∏',
 '&prop;'   : '∝',
 '&psi;'    : 'ψ',
 '&quot;'   : '"',
 '&rArr;'   : '⇒',
 '&radic;'  : '√',
 '&rang;'   : '〉',
 '&raquo;'  : '»',
 '&rarr;'   : '→',
 '&rceil;'  : '⌉',
 '&rdquo;'  : '”',
 '&real;'   : 'ℜ',
 '&reg;'    : '®',
 '&rfloor;' : '⌋',
 '&rho;'    : 'ρ',
 '&rlm;'    : 'rlm',
 '&rsaquo;' : '›',
 '&rsquo;'  : '’',
 '&sbquo;'  : '‚',
 '&scaron;' : 'š',
 '&sdot;'   : '⋅',
 '&sect;'   : '§',
 '&shy;'    : 'shy',
 '&sigma;'  : 'σ',
 '&sigmaf;' : 'ς',
 '&sim;'    : '∼',
 '&spades;' : '♠',
 '&sub;'    : '⊂',
 '&sube;'   : '⊆',
 '&sum;'    : '∑',
 '&sup1;'   : '¹',
 '&sup2;'   : '²',
 '&sup3;'   : '³',
 '&sup;'    : '⊃',
 '&supe;'   : '⊇',
 '&szlig;'  : 'ß',
 '&tau;'    : 'τ',
 '&there4;' : '∴',
 '&theta;'  : 'θ',
 '&thetasym;': 'ϑ',
 '&thinsp;' : 'thsp',
 '&thorn;'  : 'þ',
 '&tilde;'  : '˜',
 '&times;'  : '×',
 '&trade;'  : '™',
 '&uArr;'   : '⇑',
 '&uacute;' : 'ú',
 '&uarr;'   : '↑',
 '&ucirc;'  : 'û',
 '&ugrave;' : 'ù',
 '&uml;'    : '¨',
 '&upsih;'  : 'ϒ',
 '&upsilon;': 'υ',
 '&uuml;'   : 'ü',
 '&weierp;' : '℘',
 '&xi;'     : 'ξ',
 '&yacute;' : 'ý',
 '&yen;'    : '¥',
 '&yuml;'   : 'ÿ',
 '&zeta;'   : 'ζ',
 '&zwj;'    : 'zwj',
 '&zwnj;'   : 'zwnj',
 # 'α'        : 'alpha',
 # 'β'        : 'beta'
}

def create_new_xml_from_element(element):
    return etree.fromstring(etree.tostring(element))

def get_children_with_tag(elem,tag):
    return [ x for x in elem.getchildren() if(x.tag == tag) ]

def get_OtherIDs(dato, root):
    OtherIDs = get_children_with_tag(root, 'PubmedData')[0]
    OtherIDs = get_children_with_tag(OtherIDs,'ArticleIdList')[0]
    OtherIDs = get_children_with_tag(OtherIDs,'ArticleId')
    dato['OtherIDs'] = [
        {
            'Source'    : id.get('IdType').strip() if id.get('IdType') else '',
            'id'        : id.text.strip() if id.text else '',
        } for id in OtherIDs
    ]
    for OtherID in OtherIDs:
        OtherID.getparent().remove(OtherID)

def get_MeshHeadings(dato, elem):
    MeshHeadingLists        = get_children_with_tag(elem, 'MeshHeadingList')
    dato['MeshHeadings']    = []
    for MeshHeadingList in MeshHeadingLists:
        MeshHeadings = get_children_with_tag(MeshHeadingList, 'MeshHeading')
        for MeshHeading in MeshHeadings:
            mh = []
            for item in MeshHeading.getchildren():
                mh.append({
                    'Label':        item.tag.strip(),
                    'text':         item.text.strip(),
                    'UI':           item.get('UI').strip(),
                    'MajorTopicYN': item.get('MajorTopicYN').strip(),
                    'Type':         item.get('Type').strip() if (item.get('Type') is not None) else '',
                })
            dato['MeshHeadings'].append(mh)
            MeshHeading.getparent().remove(MeshHeading)
        MeshHeadingList.getparent().remove(MeshHeadingList)

def get_Authors(dato, Article):
    AuthorList = get_children_with_tag(Article, 'AuthorList')
    if(len(AuthorList)>0):
        AuthorList = AuthorList[0]
        Authors     = get_children_with_tag(AuthorList, 'Author')
        dato['Authors'] = []
        for Author in Authors:
            au = {
                    'LastName':         get_children_with_tag(Author, 'LastName')[0].text.strip() if (len(get_children_with_tag(Author, 'LastName'))>0) else '',
                    'ForeName':         get_children_with_tag(Author, 'ForeName')[0].text.strip() if (len(get_children_with_tag(Author, 'ForeName'))>0) else '',
                    'Initials':         get_children_with_tag(Author, 'Initials')[0].text.strip() if (len(get_children_with_tag(Author, 'Initials'))>0) else '',
                    'AffiliationInfo':  [
                        get_element_lower_text(af_inf).strip()
                        for af_inf in get_children_with_tag(Author, 'AffiliationInfo')
                    ],
                    'CollectiveName':   get_children_with_tag(Author, 'CollectiveName')[0].text.strip() if (len(get_children_with_tag(Author, 'CollectiveName'))>0) else '',
            }
            dato['Authors'].append(au)
            Author.getparent().remove(Author)
        AuthorList.getparent().remove(AuthorList)

def get_PersonalNameSubjectList(dato, elem):
    PersonalNameSubjectList = get_children_with_tag(elem, 'PersonalNameSubjectList')
    if(len(PersonalNameSubjectList)>0):
        PersonalNameSubjectList = PersonalNameSubjectList[0]
        PersonalNameSubjects     = get_children_with_tag(PersonalNameSubjectList, 'PersonalNameSubjects')
        dato['PersonalNameSubjects'] = []
        for PersonalNameSubject in PersonalNameSubjects:
            au = {
                    'LastName': get_children_with_tag(PersonalNameSubject, 'LastName')[0].text.strip() if (len(get_children_with_tag(PersonalNameSubject, 'LastName'))>0) else '',
                    'ForeName': get_children_with_tag(PersonalNameSubject, 'ForeName')[0].text.strip() if (len(get_children_with_tag(PersonalNameSubject, 'ForeName'))>0) else '',
                    'Initials': get_children_with_tag(PersonalNameSubject, 'Initials')[0].text.strip() if (len(get_children_with_tag(PersonalNameSubject, 'Initials'))>0) else '',
            }
            dato['PersonalNameSubjects'].append(au)
            PersonalNameSubject.getparent().remove(PersonalNameSubject)
        PersonalNameSubjectList.getparent().remove(PersonalNameSubjectList)

def get_Abstract(dato, Article):
    text = ''
    dato['AbstractSegments'] = []
    for Abstract in get_children_with_tag(Article, 'Abstract'):
        for item in Abstract.getchildren():
            if(item.tag.strip() != 'CopyrightInformation'):
                Label       = item.get('Label')
                NlmCategory = item.get('NlmCategory')
                dato['AbstractSegments'].append(
                    {
                        'Label'         : Label,
                        'NlmCategory'   : NlmCategory,
                        'text'          : get_element_lower_text(item).strip()
                    }
                )
                if(Label is not None):
                    text += '\n'+Label.strip()+'\n\n'
                text += get_element_lower_text(item).strip()+'\n'
            item.getparent().remove(item)
        if (len(Abstract.getchildren()) == 0):
            Abstract.getparent().remove(Abstract)
    dato['AbstractText'] = text.strip()

def get_ArticleDate(dato, Article):
    ArticleDate = get_children_with_tag(Article, 'ArticleDate')
    if(len(ArticleDate)>0):
        ArticleDate = ArticleDate[0]
        dato['ArticleDate']  = get_children_with_tag(ArticleDate,'Day')[0].text.strip() + '/' \
                               +get_children_with_tag(ArticleDate,'Month')[0].text.strip() + '/' \
                               +get_children_with_tag(ArticleDate,'Year')[0].text.strip()
        ArticleDate.getparent().remove(ArticleDate)

def get_PublicationTypeList(dato, Article):
    try:
        dato['PublicationTypes'] = []
        for PublicationTypeList in get_children_with_tag(Article, 'PublicationTypeList'):
            for PublicationType in get_children_with_tag(PublicationTypeList, 'PublicationType'):
                dato['PublicationTypes'].append(
                    {
                        'UI':   PublicationType.get('UI').strip(),
                        'Type': PublicationType.text.strip(),
                    }
                )
                PublicationType.getparent().remove(PublicationType)
            if (len(PublicationTypeList.getchildren()) == 0):
                PublicationTypeList.getparent().remove(PublicationTypeList)
    except:
        dato['PublicationTypes'] = []

def get_CommentsCorrectionsList(dato, elem):
    CommentsCorrectionsList = get_children_with_tag(elem, 'CommentsCorrectionsList')
    if(len(CommentsCorrectionsList)>0):
        CommentsCorrectionsList = CommentsCorrectionsList[0]
        dato['references'] = []
        for CommentsCorrections in get_children_with_tag(CommentsCorrectionsList, 'CommentsCorrections'):
            dato['references'].append(
                {
                    'RefType'   : CommentsCorrections.get('RefType').strip(),
                    'RefSource' : get_children_with_tag(CommentsCorrections, 'RefSource')[0].text.strip() if (len(get_children_with_tag(CommentsCorrections, 'RefSource'))>0 and get_children_with_tag(CommentsCorrections, 'RefSource')[0].text is not None) else '',
                    'PMID'      : get_children_with_tag(CommentsCorrections, 'PMID')[0].text.strip() if (len(get_children_with_tag(CommentsCorrections, 'PMID'))>0 and get_children_with_tag(CommentsCorrections, 'PMID')[0].text is not None) else '',
                    'Note'      : get_children_with_tag(CommentsCorrections, 'Note')[0].text.strip() if (len(get_children_with_tag(CommentsCorrections, 'Note'))>0 and get_children_with_tag(CommentsCorrections, 'Note')[0].text is not None) else '',
                }
            )
            CommentsCorrections.getparent().remove(CommentsCorrections)
        CommentsCorrectionsList.getparent().remove(CommentsCorrectionsList)

def get_Pagination(dato, Article):
    dato['Pagination'] = []
    for Pagination in get_children_with_tag(Article, 'Pagination'):
        meds                = get_children_with_tag(Pagination, 'MedlinePgn')
        if(len(meds)>0):
            try:
                dato['Pagination'].append(meds[0].text.strip())
            except:
                None
        Pagination.getparent().remove(Pagination)

def get_Languages(dato, Article):
    dato['Language'] = []
    for Language in get_children_with_tag(Article, 'Language'):
        dato['Language'].append(Language.text.strip())
        Language.getparent().remove(Language)

def get_GrantList(dato, Article):
    dato['Grants'] = []
    GrantLists = get_children_with_tag(Article, 'GrantList')
    for GrantList in GrantLists:
        Grants = get_children_with_tag(GrantList, 'Grant')
        for Grant in Grants:
            dato['Grants'].append(
                {
                    'GrantID': get_children_with_tag(Grant, 'GrantID')[0].text.strip() if (len(get_children_with_tag(Grant, 'GrantID')) > 0 and get_children_with_tag(Grant, 'GrantID')[0].text is not None) else '',
                    'Agency':  get_children_with_tag(Grant, 'Agency')[0].text.strip(),
                    'Country': get_children_with_tag(Grant, 'Country')[0].text.strip() if (len(get_children_with_tag(Grant, 'Country')) > 0 and get_children_with_tag(Grant, 'Country')[0].text is not None) else '',
                }
            )
            Grant.getparent().remove(Grant)
        GrantList.getparent().remove(GrantList)

def get_KeywordList(dato, elem):
    KeywordList = get_children_with_tag(elem, 'KeywordList')
    if(len(KeywordList)>0):
        KeywordList = KeywordList[0]
        dato['Keywords'] = []
        for Keyword in get_children_with_tag(KeywordList, 'Keyword'):
            if(Keyword.text is not None):
                dato['Keywords'].append(Keyword.text.strip())
            Keyword.getparent().remove(Keyword)
        KeywordList.getparent().remove(KeywordList)

def get_ChemicalList(dato, elem):
    ChemicalList = get_children_with_tag(elem, 'ChemicalList')
    if(len(ChemicalList)>0):
        ChemicalList = ChemicalList[0]
        dato['Chemicals'] = []
        for Chemical in get_children_with_tag(ChemicalList, 'Chemical'):
            dato['Chemicals'].append(
                {
                    'RegistryNumber'    : get_children_with_tag(Chemical, 'RegistryNumber')[0].text.strip(),
                    'NameOfSubstance'   : get_children_with_tag(Chemical, 'NameOfSubstance')[0].text.strip(),
                    'UI'                : get_children_with_tag(Chemical, 'NameOfSubstance')[0].get('UI').strip(),
                }
            )
            Chemical.getparent().remove(Chemical)
        ChemicalList.getparent().remove(ChemicalList)

def get_OtherAbstract(dato, Article):
    OtherAbstract = get_children_with_tag(Article, 'OtherAbstract')
    if(len(OtherAbstract)>0):
        OtherAbstract = OtherAbstract[0]
        dato['OtherAbstract'] = {
            'Type'      : OtherAbstract.get('Type').strip(),
            'Language'  : OtherAbstract.get('Language').strip(),
            'text'      : get_element_lower_text(OtherAbstract).strip(),
        }
        OtherAbstract.getparent().remove(OtherAbstract)

def get_SupplMeshList(dato, elem):
    dato['SupplMeshList']    = []
    MeshHeadingLists        = get_children_with_tag(elem, 'SupplMeshList')
    for MeshHeadingList in MeshHeadingLists:
        MeshHeadings = get_children_with_tag(MeshHeadingList, 'SupplMeshName')
        for MeshHeading in MeshHeadings:
            dato['SupplMeshList'].append({
                'text'  : MeshHeading.text.strip(),
                'Type'  : MeshHeading.get('Type').strip(),
                'UI'    : MeshHeading.get('UI').strip(),
            })
            MeshHeading.getparent().remove(MeshHeading)
        MeshHeadingList.getparent().remove(MeshHeadingList)

def get_InvestigatorList(dato, Article):
    InvestigatorList = get_children_with_tag(Article, 'InvestigatorList')
    if(len(InvestigatorList)>0):
        InvestigatorList = InvestigatorList[0]
        Investigators     = get_children_with_tag(InvestigatorList, 'Investigator')
        dato['Investigators'] = []
        for Investigator in Investigators:
            au = {
                    'LastName': get_children_with_tag(Investigator, 'LastName')[0].text.strip() if (len(get_children_with_tag(Investigator, 'LastName'))>0) else '',
                    'ForeName': get_children_with_tag(Investigator, 'ForeName')[0].text.strip() if (len(get_children_with_tag(Investigator, 'ForeName'))>0) else '',
                    'Initials': get_children_with_tag(Investigator, 'Initials')[0].text.strip() if (len(get_children_with_tag(Investigator, 'Initials'))>0) else '',
                    'AffiliationInfo': [
                        get_element_lower_text(af_inf).strip()
                        for af_inf in get_children_with_tag(Investigator, 'AffiliationInfo')
                    ],
                    'CollectiveName': get_children_with_tag(Investigator, 'CollectiveName')[0].text.strip() if (len(get_children_with_tag(Investigator, 'CollectiveName'))>0) else '',
            }
            dato['Investigators'].append(au)
            Investigator.getparent().remove(Investigator)
        InvestigatorList.getparent().remove(InvestigatorList)

def get_NumberOfReferences(dato, Article):
    NumberOfReferences = get_children_with_tag(Article, 'NumberOfReferences')
    if (len(NumberOfReferences) > 0):
        NumberOfReferences = NumberOfReferences[0]
        dato['NumberOfReferences'] = NumberOfReferences.text.strip()
        NumberOfReferences.getparent().remove(NumberOfReferences)

def get_element_lower_text(element, joiner=' '):
    r2  =  create_new_xml_from_element(element)
    return joiner.join(r2.xpath("//text()")).replace('\n',' ')

def get_pmid(dato, elem):
    pmid = get_children_with_tag(elem, 'PMID')[0]
    dato['pmid'] = pmid.text.strip()
    pmid.getparent().remove(pmid)

def get_CitationSubset(dato, elem):
    CitationSubset = get_children_with_tag(elem, 'CitationSubset')
    if(len(CitationSubset)>0):
        CitationSubset = CitationSubset[0]
        dato['CitationSubset'] = CitationSubset.text.strip()
        CitationSubset.getparent().remove(CitationSubset)

def get_DateCreated(dato, elem):
    ttt = get_children_with_tag(elem,'DateCreated')
    if(len(ttt)>0):
        DateCreated   = ttt[0]
        dato['DateCreated']  = get_children_with_tag(DateCreated,'Day')[0].text.strip() + '/' +get_children_with_tag(DateCreated,'Month')[0].text.strip() + '/' +get_children_with_tag(DateCreated,'Year')[0].text.strip()
        DateCreated.getparent().remove(DateCreated)
    return None

def get_DateRevised(dato, elem):
    DateRevised = get_children_with_tag(elem, 'DateRevised')
    if(len(DateRevised)>0):
        DateRevised = DateRevised[0]
        dato['DateRevised']  = get_children_with_tag(DateRevised,'Day')[0].text.strip() + '/' +get_children_with_tag(DateRevised,'Month')[0].text.strip() + '/' +get_children_with_tag(DateRevised,'Year')[0].text.strip()
        DateRevised.getparent().remove(DateRevised)

def get_DateCompleted(dato, elem):
    DateCompleted = get_children_with_tag(elem,'DateCompleted')
    if(len(DateCompleted)>0):
        DateCompleted = DateCompleted[0]
        dato['DateCompleted'] = get_children_with_tag(DateCompleted, 'Day')[0].text.strip() + '/' + \
                                get_children_with_tag(DateCompleted, 'Month')[0].text.strip() + '/' + \
                                get_children_with_tag(DateCompleted, 'Year')[0].text.strip()
        DateCompleted.getparent().remove(DateCompleted)

def get_ArticleTitle(dato, Article):
    if(len(get_children_with_tag(Article,'ArticleTitle'))>0):
        ArticleTitle = get_children_with_tag(Article, 'ArticleTitle')[0]
        try:
            dato['ArticleTitle'] = ArticleTitle.text.strip()
        except:
            dato['ArticleTitle'] = ''
        ArticleTitle.getparent().remove(ArticleTitle)

def get_MedlineJournalInfo(dato, Article):
    try:
        dato['MedlineJournalInfo'] = {}
        ch1 = get_children_with_tag(Article, 'MedlineJournalInfo')
        if(len(ch1)>0):
            dato['MedlineJournalInfo'] = {
                'Country':      get_children_with_tag(ch1[0], 'Country')[0].text.strip(),
                'MedlineTA':    get_children_with_tag(ch1[0], 'MedlineTA')[0].text.strip(),
                'NlmUniqueID':  get_children_with_tag(ch1[0], 'NlmUniqueID')[0].text.strip(),
                'ISSNLinking':  get_children_with_tag(ch1[0], 'ISSNLinking')[0].text.strip()
            }
            if(len(ch1[0].getchildren())==0):
                ch1[0].getparent().remove(ch1[0])
    except:
        print(traceback.format_exc())
        dato['MedlineJournalInfo'] = {}

def get_DataBankList(dato, Article):
    try:
        dato['DataBankList'] = []
        for DataBankList in get_children_with_tag(Article, 'DataBankList'):
            for DataBank in get_children_with_tag(DataBankList, 'DataBank'):
                AccessionNumbers = []
                for AccessionNumberList in get_children_with_tag(DataBank, 'AccessionNumberList'):
                    for AccessionNumber in get_children_with_tag(AccessionNumberList, 'AccessionNumber'):
                        AccessionNumbers.append(AccessionNumber.text.strip())
                        AccessionNumber.getparent().remove(AccessionNumber)
                    AccessionNumberList.getparent().remove(AccessionNumberList)
                AccessionNumbers = list(set(AccessionNumbers))
                db_name          = get_children_with_tag(DataBank, 'DataBankName')
                dato['DataBankList'].append(
                    {
                        'DataBankName'      : db_name[0].text.strip(),
                        'AccessionNumbers'  : AccessionNumbers
                    }
                )
                db_name[0].getparent().remove(db_name[0])
                DataBank.getparent().remove(DataBank)
            if(len(DataBankList.getchildren())==0):
                DataBankList.getparent().remove(DataBankList)
    except:
        dato['DataBankList'] = {}

def get_ELocationID(dato, Article):
    dato['ELocationIDs'] = []
    ELocationIDs = get_children_with_tag(Article, 'ELocationID')
    for ELocationID in ELocationIDs:
        dato['ELocationIDs'].append(
            {
                'EIdType'   : ELocationID.get('EIdType').strip(),
                'ValidYN'   : ELocationID.get('ValidYN').strip(),
                'value'     : ELocationID.text.strip()
            }
        )
        ELocationID.getparent().remove(ELocationID)

def get_Journal_info(dato, Article):
    try:
        dato['Journal'] = {}
        ch1 = get_children_with_tag(Article, 'Journal')
        if(len(ch1)>0):
            ch1     = ch1[0]
            tit     = get_children_with_tag(ch1, 'Title')[0]
            #
            iso     = get_children_with_tag(ch1, 'ISOAbbreviation')
            if(len(iso)>0):
                iso_text = iso[0].text.strip()
                iso[0].getparent().remove(iso[0])
            else:
                iso_text = None
            #
            issn    = get_children_with_tag(ch1, 'ISSN')
            if(len(issn)>0):
                issn_text = issn[0].text.strip()
                issn[0].getparent().remove(issn[0])
            else:
                issn_text = None
            dato['Journal'] = {
                'Title'             : tit.text.strip(),
                'ISOAbbreviation'   : iso_text,
                'ISSN'              : issn_text,
                'JournalIssue'      : {}
            }
            ji      = get_children_with_tag(ch1, 'JournalIssue')[0]
            try:
                vol = get_children_with_tag(ji, 'Volume')[0].text.strip()
            except:
                vol = None
            jdate       = get_children_with_tag(ji, 'PubDate')[0]
            med_date    = get_children_with_tag(jdate, 'MedlineDate')
            if(len(med_date)>0):
                le_date = med_date[0].text.strip()
            else:
                try:
                    dd  = get_children_with_tag(jdate, 'Day')[0].text.strip()
                except:
                    dd  = 15
                try:
                    mm  = get_children_with_tag(jdate, 'Month')[0].text.strip()
                except:
                    mm  = 6
                yy      = get_children_with_tag(jdate, 'Year')[0].text.strip()
                try:
                    le_date = "{} {} {}".format(dd, mm, yy)
                    le_date = parser.parse(le_date, dayfirst=True)
                    le_date = le_date.strftime("%d-%m-%Y")
                except:
                    le_date = "{} {} {}".format(dd, mm, yy)
            issue   = get_children_with_tag(ji, 'Issue')
            dato['Journal']['JournalIssue'] = {
                'Volume'    : vol,
                'Issue'     : issue[0].text.strip() if(len(issue)>0) else '',
                'PubDate'   : le_date
            }
            if(len(issue) > 0):
                issue[0].getparent().remove(issue[0])
            jdate.getparent().remove(jdate)
            if(vol is not None):
                vol = get_children_with_tag(ji, 'Volume')[0]
                vol.getparent().remove(vol)
            tit.getparent().remove(tit)
            if(len(ji.getchildren())==0):
                ji.getparent().remove(ji)
            if(len(ch1.getchildren())==0):
                ch1.getparent().remove(ch1)
    except:
        print(traceback.format_exc())
        dato['Journal'] = {}

def get_VernacularTitle(dato, Article):
    dato['VernacularTitle'] = None
    vt = get_children_with_tag(Article, 'VernacularTitle')
    if(len(vt)>0):
        dato['VernacularTitle'] = vt[0].text.strip()
        vt[0].getparent().remove(vt[0])

def replace_html_special(text):
    for special in html_special:
        if special.decode('utf-8') in text:
            print(text)
            print(special.decode('utf-8'))
            text = text.replace(special, html_special[special].decode('utf-8'))
            # text = text.replace(special, ' _{}_ '.format(special.replace('&','').replace(';','')))
            print(text)
            print(40*'-')
    return text

def do_for_one_pmid(root):
    dato        = {}
    elem        = get_children_with_tag(root, 'MedlineCitation')[0]
    Article     = get_children_with_tag(elem, 'Article')[0]
    try:
        get_pmid(dato, elem)
        get_CitationSubset(dato, elem)
        get_DateCreated(dato, elem)
        get_DateRevised(dato, elem)
        get_NumberOfReferences(dato, Article)
        get_InvestigatorList(dato, Article)
        get_SupplMeshList(dato, elem)
        get_ChemicalList(dato, elem)
        get_MeshHeadings(dato, elem)
        get_PersonalNameSubjectList(dato, elem)
        get_OtherIDs(dato, root)
        get_KeywordList(dato, elem)
        get_CommentsCorrectionsList(dato, elem)
        get_GrantList(dato, Article)
        get_Languages(dato, Article)
        get_PublicationTypeList(dato, Article)
        get_Pagination(dato, Article)
        get_DateCompleted(dato, elem)
        get_ArticleTitle(dato, Article)
        get_Authors(dato, Article)
        get_ArticleDate(dato, Article)
        get_Abstract(dato, Article)
        get_OtherAbstract(dato, Article)
        get_MedlineJournalInfo(dato, Article)
        get_DataBankList(dato, Article)
        get_ELocationID(dato, Article)
        get_Journal_info(dato, Article)
        get_VernacularTitle(dato, Article)
        if (len(Article.getchildren()) > 0):
            print(dato['pmid'])
            print(etree.tostring(Article, pretty_print=True))
    except:
        print(etree.tostring(elem, pretty_print=True))
        traceback.print_exc()
        tb = traceback.format_exc()
        print(tb)
    if 'ArticleTitle' in dato:
        dato['ArticleTitle']    = replace_html_special(dato['ArticleTitle'])
    if 'AbstractText' in dato:
        dato['AbstractText']    = replace_html_special(dato['AbstractText'])
    if 'AbstractSegments' in dato:
        for seg in dato['AbstractSegments']:
            seg['text']         = replace_html_special(seg['text'])
    return dato

def create_pmid_body(pmid):
    return {
        "query": {
            "constant_score": {
                "filter": {
                    'bool': {
                        "must": [
                            {
                                "term": {
                                    "pmid": pmid
                                }
                            },
                        ]
                    }
                }
            }
        }
    }

def create_an_action(elk_dato, the_id):
    if(the_id is None):
        pass
    else:
        elk_dato['_id']  = the_id
    ################
    elk_dato['_op_type'] = u'index'
    elk_dato['_index']   = index
    elk_dato['_type']    = doc_type
    return elk_dato

def upload_to_elk(finished=False):
    global actions
    global b_size
    if(len(actions) >= b_size) or (len(actions)>0 and finished):
        flag = True
        while (flag):
            try:
                result = bulk(es, iter(actions))
                pprint(result)
                flag = False
            except Exception as e:
                print(e)
                if ('ConnectionTimeout' in str(e)):
                    print('Retrying')
                else:
                    flag = False
        actions = []

def replace_weird_stuff(content):
    content = content.replace('<sub>', ' __sub__ ')
    content = content.replace('<SUB>', ' __sub__ ')
    content = content.replace('</sub>', ' __end_sub__ ')
    content = content.replace('</SUB>', ' __end_sub__ ')
    #
    content = content.replace('<sup>', ' __sup__ ')
    content = content.replace('<SUP>', ' __sup__ ')
    content = content.replace('</sup>', ' __end_sup__ ')
    content = content.replace('</SUP>', ' __end_sup__ ')
    #
    content = content.replace('<i>', ' __i_tag__ ')
    content = content.replace('<I>', ' __i_tag__ ')
    content = content.replace('</i>', ' __end_i_tag__ ')
    content = content.replace('</I>', ' __end_i_tag__ ')
    #
    content = content.replace('<underline>', ' __underline__ ')
    content = content.replace('<UNDERLINE>', ' __underline__ ')
    content = content.replace('</underline>', ' __end_underline__ ')
    content = content.replace('</UNDERLINE>', ' __end_underline__ ')
    #
    content = content.replace('<bold>', ' __bold__ ')
    content = content.replace('<BOLD>', ' __bold__ ')
    content = content.replace('</bold>', ' __end_bold__ ')
    content = content.replace('</BOLD>', ' __end_bold__ ')
    #
    content = content.replace('<italic>', ' __italic__ ')
    content = content.replace('<ITALIC>', ' __italic__ ')
    content = content.replace('</italic>', ' __end_italic__ ')
    content = content.replace('</ITALIC>', ' __end_italic__ ')
    return content

def upload_to_elk_joint(finished=False):
    global actions_joint
    global b_size_joint
    if(len(actions_joint) >= b_size_joint) or (len(actions_joint)>0 and finished):
        flag = True
        while (flag):
            try:
                result = bulk(es, iter(actions_joint))
                pprint(result)
                flag = False
            except Exception as e:
                print(e)
                if ('ConnectionTimeout' in str(e)):
                    print('Retrying')
                else:
                    flag = False
        actions_joint = []

def create_an_action_joint(elk_dato, the_id, index, doc_type):
    if(id is None):
        pass
    else:
        elk_dato['_id']  = the_id
    ################
    elk_dato['_op_type'] = u'index'
    elk_dato['_index']   = index
    elk_dato['_type']    = doc_type
    return elk_dato

index_joint     = 'pubmed_abstracts_joint_0_1'
doc_type_joint  = 'abstract_map_joint_0_1'

es = Elasticsearch([
        # ELasticsearch INGESTORS
        'localhost:9200'
    ],
    verify_certs=True,
    timeout=150,
    max_retries=10,
    retry_on_timeout=True
)

index       = 'pubmed_abstracts_0_1'
doc_type    = 'abstract_map_0_1'

din         = sys.argv[1]
gr_eq       = int(sys.argv[2])
l_eq        = int(sys.argv[3])
year        = '23'

fs          = [
    din+f for f in os.listdir(din)
    if (
        f.endswith('.xml.gz')
        and
        int(f.replace('pubmed{}n'.format(year),'').replace('.xml.gz','')) >= gr_eq
        and
        int(f.replace('pubmed{}n'.format(year),'').replace('.xml.gz','')) <= l_eq
    )
]
fs.sort(reverse=False)

fc      = 0
b_size  = 100
b_size_joint = 100
actions = []
actions_joint = []
for file_gz in fs: #[fromm:too]:
    fc += 1
    print(file_gz)
    infile      = gzip.open(file_gz)
    content     = infile.read()
    content     = replace_weird_stuff(content)
    children    = etree.fromstring(content).getchildren()
    ch_counter  = 0
    pbar = tqdm(children)
    for ch_tree in pbar:
        ch_counter += 1
        for elem in ch_tree.iter(tag='PubmedArticle'):
            dato        = do_for_one_pmid(elem)
            if('pmid' in dato):
                if ('DateCompleted' not in dato):
                    if ('ArticleDate' in dato):
                        dato['DateCompleted'] = dato['ArticleDate']
                    else:
                        dato['DateCompleted'] = dato['DateRevised']
                # bod = create_pmid_body(dato['pmid'])
                # total_found = es.search(index=index, doc_type=doc_type, body=bod)['hits']['total']
                # if (total_found == 0):
                actions.append(create_an_action(dato, dato['pmid']))
                upload_to_elk(finished=False)
                ####################################################################################################
                jt1 = dato['ArticleTitle'].strip() if ('ArticleTitle' in dato) else ''
                jt2 = dato['AbstractText'].strip() if ('AbstractText' in dato) else ''
                if (len(jt2) > 0):
                    jt = jt1 + '\n--------------------\n' + jt2
                    jt = jt.strip()
                    simple_datum = {
                        'joint_text'    : jt,
                        'DateCompleted' : dato['DateCompleted'],
                        'DateRevised'   : dato['DateRevised'] if ('DateRevised' in dato) else None,
                        'ArticleDate'   : dato['ArticleDate'] if ('ArticleDate' in dato) else None,
                        'pmid'          : dato['pmid'],
                        'Chemicals'     : dato['Chemicals'] if ('Chemicals' in dato) else None,
                        'OtherIDs'      : dato['OtherIDs'] if ('OtherIDs' in dato) else None,
                        'MeshHeadings'  : dato['MeshHeadings'] if ('MeshHeadings' in dato) else None,
                        'Keywords'      : dato['Keywords'] if ('Keywords' in dato) else None,
                        'SupplMeshList' : dato['SupplMeshList'] if ('SupplMeshList' in dato) else None,
                    }
                    actions_joint.append(
                        create_an_action_joint(
                            simple_datum,
                            simple_datum['pmid'],
                            index_joint,
                            doc_type_joint
                        )
                    )
                    upload_to_elk_joint(finished=False)
                ####################################################################################################
        pbar.set_description('{} finished {} of {} trees. {} of {} files {} actions.'.format(
            file_gz.split(os.path.sep)[-1],
            ch_counter, len(children), fc, len(fs), len(actions))
        )
    if (len(actions) > 0):
        upload_to_elk(finished=True)
        upload_to_elk_joint(finished=True)
