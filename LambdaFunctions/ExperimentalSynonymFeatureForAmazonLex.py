import requests
import bs4 as bs




def wordTosynonym(word):
    
    try:
        
        syn = []
        URL=f'https://www.sinonimosonline.com/{word.lower()}'
        page=requests.get(URL)
        miSopita=bs.BeautifulSoup(page.content, "html.parser")
        results=miSopita.find(class_="synonim")
        final_filter=results.find_all("a","sinonimo")
        
        for synonym in final_filter:
            print("=================")
            print(synonym.text)
            print("=================")
            syn.append(synonym.text)
        
        formatedList=formatForAmazonLexSDK(syn)
            
    except:
        
        #In case word isnt found
        formatedList=[]    
        
    return formatedList

def processStringList(synomyms):
    
    #For compatibility with other websites
    processStringSyn=synomyms.replace("  ","")
    li=list(processStringSyn.split(","))
    return li    

def formatForAmazonLexSDK(synonymsList):
    
    synonyms=[]
    
    for synonym in synonymsList:
        synonymMap={
            'value':synonym
        }
        synonyms.append(synonymMap)
    
    return synonyms


