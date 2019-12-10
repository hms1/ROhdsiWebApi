# @file Vocabulary
#
# Copyright 2019 Observational Health Data Sciences and Informatics
#
# This file is part of ROhdsiWebApi
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


.getSourceConceptId <- function(baseUrl, sourceCode, sourceVocabularyId) {
  url <- sprintf("%s/vocabulary/vocab/search/%s", baseUrl, sourceCode)
  json <- httr::GET(url)
  json <- httr::content(json)
  
  
  sourceConcept <- json[sapply(json, function(j) {
    j$INVALID_REASON == "V" &
      j$VOCABULARY_ID == sourceVocabularyId &
      j$CONCEPT_CODE == sourceCode
  })]
  sourceConcept[[1]]
}

.sourceToStandard <- function(baseUrl, sourceConceptId) {
  json <- .getRelatedConcepts(baseUrl, sourceConceptId)
  
  json[sapply(json, function(j) {
    j$STANDARD_CONCEPT == "S"
  })]
}

.getRelatedConcepts <- function(baseUrl, conceptId) {
  url <- sprintf("%s/vocabulary/vocab/concept/%s/related", baseUrl, conceptId)
  json <- httr::GET(url)
  httr::content(json)
}

.getMaxDistance <- function(relationships) {
  maxDistance <- 0
  for (r in relationships) {
    if (r$RELATIONSHIP_DISTANCE > maxDistance) {
      maxDistance <- r$RELATIONSHIP_DISTANCE
    }
  }
  maxDistance
}

.getPreferredMedDraTerm <- function(baseUrl, sourceCode, sourceVocabularyId) {
  sourceConcept <- .getSourceConceptId(baseUrl, sourceCode, sourceVocabularyId)
  standardConcepts <- .sourceToStandard(baseUrl, sourceConcept$CONCEPT_ID)
  
  meddraTerms <- lapply(standardConcepts, function(sc) {
    relatedConcepts <- lapply(sc$CONCEPT_ID, function(rc) {
      .getRelatedConcepts(baseUrl, rc)
    })
    
    # get all of the matches within each set
    preferredTerms <- lapply(relatedConcepts, function(rc) {
      matches <- rc[sapply(rc, function(j) {
        j$VOCABULARY_ID == "MedDRA" & j$CONCEPT_CLASS_ID == "PT" & j$RELATIONSHIP_CAPTION == "Has ancestor of"
      })]
      
      results <- lapply(matches, function(m) {
        distance <- data.frame(
          CONCEPT_ID = m$CONCEPT_ID,
          CONCEPT_NAME = m$CONCEPT_NAME,
          CONCEPT_CODE = m$CONCEPT_CODE,
          DISTANCE = .getMaxDistance(m$RELATIONSHIPS),
          stringsAsFactors = FALSE
        )
      })
      result <- do.call(rbind.data.frame, results)
      result <- result[result$DISTANCE == 1,]
      
      if (nrow(result) > 0) {
        result$SOURCE_CONCEPT_ID <- sourceConcept$CONCEPT_ID
        result$SOURCE_CODE <- sourceCode
        result$SOURCE_CODE_NAME <- sourceConcept$CONCEPT_NAME
        result$SOURCE_VOCABULARY_ID <- sourceVocabularyId
        result$SNOMED_CONCEPT_ID <- sc$CONCEPT_ID
        result$SNOMED_CODE <- sc$CONCEPT_ID
        result$SNOMED_NAME <- sc$CONCEPT_NAME
        
        dplyr::select(result,
                      SOURCE_CONCEPT_ID,
                      SOURCE_CODE,
                      SOURCE_CODE_NAME,
                      SOURCE_VOCABULARY_ID,
                      SNOMED_CONCEPT_ID = SNOMED_CONCEPT_ID,
                      SNOMED_CODE = SNOMED_CODE,
                      SNOMED_NAME = SNOMED_NAME,
                      MEDDRA_CONCEPT_ID = CONCEPT_ID,
                      MEDDRA_CODE = CONCEPT_CODE,
                      MEDDRA_NAME = CONCEPT_NAME)  
      } else {
        data.frame()
      }
    })
    do.call(rbind.data.frame, preferredTerms)
  })
  
  do.call(rbind.data.frame, meddraTerms)
}


#' Get MedDRA terms from 1 source code
#' 
#' @details Obtain MedDRA Preferred Terms that are direct ancestors for a data frame of
#' condition source codes. Note: you must have MedDRA terms loaded in your OMOP Vocabulary.
#' 
#' @param sourceCode             A single source code
#' @param sourceVocabularyId     The name of the source vocabulary id for the source code (e.g. ICD9CM, ICD10CM)
#' @param baseUrl                The WebAPI endpoint
#' 
#' @export
getMedDraFromSourceCode <- function(baseUrl, sourceCode, sourceVocabularyId) {
  .getPreferredMedDraTerm(baseUrl, sourceCode, sourceVocabularyId)
}


#' Get MedDRA terms from source code data frame
#' 
#' @details Obtain MedDRA Preferred Terms that are direct ancestors for a data frame of
#' condition source codes. Note: you must have MedDRA terms loaded in your OMOP Vocabulary.
#' 
#' @param baseUrl          The WebAPI endpoint
#' @param sourceCodeDf     A data frame consisting of: SOURCE_CODE and SOURCE_VOCABULARY_ID
#' @param maxThreads       (OPTIONAL) The maximum number of parallel requests to make to WebAPI. 
#'                         Default is 10. It is advisable to not use too many.
#' 
#' @export
getMedDraFromSourceCodeDf <- function(baseUrl, sourceCodeDf, maxThreads = 10) {
  
  sourceCodes <- apply(sourceCodeDf, 1, function(s) {
    list(sourceCode = s["SOURCE_CODE"][[1]],
         sourceVocabularyId = s["SOURCE_VOCABULARY_ID"][[1]],
         baseUrl = baseUrl)
  })
  
  cluster <- ParallelLogger::makeCluster(min(maxThreads, nrow(sourceCodeDf)))
  result <- ParallelLogger::clusterApply(cluster = cluster, sourceCodes, function(s) {
    .getPreferredMedDraTerm(s$baseUrl, s$sourceCode, s$sourceVocabularyId)
  })
  ParallelLogger::stopCluster(cluster = cluster)
  
  do.call(rbind.data.frame, result)
}

