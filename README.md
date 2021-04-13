[![forthebadge](https://forthebadge.com/images/badges/built-with-love.svg)](https://forthebadge.com) [![forthebadge](https://forthebadge.com/images/badges/powered-by-coffee.svg)](https://forthebadge.com)

# Analyzing Linguistic Differences in Text Data between Two Time Periods
This project seeks to document how three corpora differ in four different core aspects of language. The three corpora are: 7 classic novels taken from Project Gutenberg (all published pre-1925), modern internet writing (from a variety of blogs and from comments on the Hacker News website), and 9 FanFictions published on FanFiction.net. We posit that the linguistic profile of a text can be elucidated using these four features. These analyses provide a comprehensive look at the texts which allows us to both quantitatively and subjectively examine how some aspects of language have changed over time.

# Why Include Fanfiction Data?
The initial goal of this project was to investigate how sentence structure, word complexity, and topics have evolved from classic prose to modern internet writing. However, these texts differ not only in their age but in medium. The Project Gutenberg documents are all examples of narrative fiction whereas the Hacker News comments and blogs are an example of short-form colloquial writing (presuming no one is writing a novel length blog). As such, the medium becomes a potential confounding variable and obscures our ability to decipher whether differences in the texts are due to changes in language over time or simply because of differences in the medium. For this reason, we chose to control for the medium by adding a third source: text from fanfiction. 

We chose fanfiction specifically because these are publicly available modern examples of narrative fiction. (Also, because short-form colloquial writing from pre-1925 is much more difficult to find). We built a web-scraper to pull the text data from a random sample of nine fanfictions from fanfiction.net. The fanfictions were selected using a random number generator, with sanity checks to make sure that the fanfictions were a good control for the quality of the Project Gutenberg stories. (The sanity checks were that the fanfiction must have over a certain threshold of followers, which indicates that it is popular and thus of good enough quality to become popular; must be over 20,000 words long but no longer than 500,000 words; and it must not contain explicit content). The fanfiction data was used as a third source in all of our analyses. 

The text files for the nine fanfictions are included in a subfolder in the fanfiction folder. 

# Terminology

Throughout our analyses, we reference the corpora by the terms: 

1. **Books:** Which encompasses the text from all of the 7 Project Gutenberg books
2. **Blogs:** Which encompasses all of the modern text; both Hacker News and the blogs (despite the name)
3. **Fanfics:** Which encompasses the text from all 9 fanfictions sourced from Fanfiction.net

# Modules 
## Data Ingestion/Cleaning 
**initSession:**   
        A script that initializes a spark session and context to establish a connection to the Apache Spark webserver.   
        
**FicScraper:** 
              A web scraper that scrapes and cleans fanfiction text data from fanfictions on fanfiction.net. The script writes the aforementioned text data into a local .txt file. 
              
**ImportTest:**  
              A script that imports text data in the form of a .txt file, cleans, and then transforms the data into a Spark RDD. Each major text type (blog, books, fanfics) has its own corresponding import / cleaning function. 
              
**TokenizeTest:**   
              A script that tokenizes the contents of an RDD output by ImportTest. Contains the function TokenizeThis, which is passed the spark context and a string argument specifying which source one would like returned (options: "Books", "Blogs", "Fanfics", "All").
              
**tidyRoutine:**     
              A script that cleans tokenized data and applies part of speech tagging and lemmatization. Contains the method TidyThis which is passed the tokenized dataframe and two string arguments specifying whether or not one would like the text to be lemmatized (options: "Yes", "No") or tagged with the parts of speech (options: "Yes", "No").          
                
**Sentencify:**     
              A script that takes in text source RDDs (or lists of RDDs) and converts each into a DF wherein row corresponds to a list of tokens in each sentence with an identifier at index 0. 
              
## Text Complexity  
**complexityAnalyses:**   
              A script that calculates Type-Token ratio, Hapax Richness, and Avg. Word Length. These analyses provide a glimpse at the lexical profile of a text. These methods were decided on after doing a literature review of text complexity measures used in linguistic analysis. Measures that were easily understood and could be computed in a distributed way were prioritized.    
               
**fleschKincaid:**    
              A script that calculates the Flesch-Kincaid score for a tokenized RDD. The Flesch-Kincaid score is a commonly used measure of the complexity of a text; colloquially, it is the score used when calculating the "reading level" of a book (i.e., when someone says a book is at a "8th grade reading level" they are referring to it's Flesch-Kincaid score). 
              
## Language Functions

**TFIDF_Code:**    
        A script that calculates the TF - IDF (term frequency–inverse document frequency) of a tokenized RDD. TF - IDF is used to illustrate how important a word or set of words are to a text sample. This data frame is then exported as a .csv to be visualized in external software.

**functionAnalyses:**    
              A script that calculates the most common nouns, pronouns, and adjectives for a text sample from Sentencify. The nouns are used to identify common characters, places, and things discussed in the text and are analyzed along with the topic analyses. 
              
**punctProportion:**  
              A script that calculates the proportion of punctuation for a tokenized RDD. 
              
**taggerFunction:**  
              A script that takes a dataframe of sentences (from Sentencify) and returns a dataframe with the POS tagged where each row corresponds to a token from a source. 

## Topic Analysis  

**LDA_Code:**  
              A script that applies LDA topic modeling on a dataframe of sentences(from Sentencify). Latent Dirichlet Allocation is a generative probabalistic model that assumes a topic is a mixture of sets of words and that each document is a set of topic probabilities. This analysis will not appear in the final document due to technical limitations encountered and general difficulties of optimizing hyperparameters, which is outside the scope of this project.   

**ngrams:**    
              A script that takes in a dataframe of sentences(from Sentencify) and calculates the most common bigrams and trigrams of word combinations within the dataframe. Ngrams are used to identify common short phrases in the texts, which elucidates “what” is being repeatedly mentioned in the text. Common trigrams (and ngrams > 2) were investigated but ultimately excluded from the analyses because the responses were noisy and did not provide any additional insights. 


## Sentiment Analysis 

**sentimentAnalysis:**   
              A script that takes in a dataframe of sentences (from Sentencify) and uses the VADER (Valence Aware Dictionary for Sentiment Reasoning) model to calculate the average polarity of the text sample. Studies have shown that VADER performs as well as human-raters at correctly identifying the underlying sentiment (positive, negative, neutral) of a text. 
              
 <p align="center"><img src="https://thumbs.dreamstime.com/t/books-many-different-colors-horizontal-line-39159446.jpg" alt="books" width="500"/></p>
