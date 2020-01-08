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


#' Get Source Concept Id from Source Code
#' 
#' @param baseUrl              The base URL for the WebApi instance, for example:
#'                             "http://server.org:80/WebAPI".
#' @param sourceCode           A source code
#' @param sourceVocabularyId   The vocabulary id that the source code belongs to
#' 
#' @return A list object defining the source concept
#' 
#' @export
getSourceConcept <- function(baseUrl, sourceCode, sourceVocabularyId) {
  url <- sprintf("%s/vocabulary/vocab/search/%s", baseUrl, sourceCode)
  json <- httr::GET(url)
  json <- httr::content(json)
  
  
  sourceConcept <- json[sapply(json, function(j) {
    j$INVALID_REASON == "V" &
      j$VOCABULARY_ID == sourceVocabularyId &
      j$CONCEPT_CODE == sourceCode
  })]
  if (length(sourceConcept) > 0) {
    sourceConcept[[1]]  
  } else {
    list()
  }
}

#' Get Mapped Standard concepts for a source concept
#' 
#' @param baseUrl              The base URL for the WebApi instance, for example:
#'                             "http://server.org:80/WebAPI".
#' @param sourceConceptId      A source concept id for which we are to find the mapped standard concepts
#' 
#' @return A list of standard concepts mapped to the source concept id
#' 
#' @export
getMappedStandardConcepts <- function(baseUrl, sourceConceptId) {
  json <- getRelatedConcepts(baseUrl, sourceConceptId)
  
  json[sapply(json, function(j) {
    j$STANDARD_CONCEPT == "S" & j$INVALID_REASON == "V"
  })]
}


#' Get Related Concepts for a concept id
#' 
#' @param baseUrl              The base URL for the WebApi instance, for example:
#'                             "http://server.org:80/WebAPI".
#' @param conceptId            A concept id for which we are to find the related concepts
#' 
#' @export
getRelatedConcepts <- function(baseUrl, conceptId) {
  url <- sprintf("%s/vocabulary/vocab/concept/%s/related", baseUrl, conceptId)
  json <- httr::GET(url)
  httr::content(json)
}

.hasRelationship <- function(relationships, relationshipId, minDistance, maxDistance) {
  matches <- relationships[sapply(relationships, function(r) {
    
    if (is.null(maxDistance)) {
      r$RELATIONSHIP_NAME == relationshipId &
        r$RELATIONSHIP_DISTANCE >= minDistance
    } else {
      r$RELATIONSHIP_NAME == relationshipId &
        r$RELATIONSHIP_DISTANCE >= minDistance &
        r$RELATIONSHIP_DISTANCE <= maxDistance  
    }
  })]
  
  length(matches) > 0
}


#' Get classification term from source code
#' 
#' @details For a source code, obtain mapped classification term(s) by traversing from source code to
#' standard concept to classification term.
#' 
#' @param baseUrl                The base URL for the WebApi instance, for example:
#'                               "http://server.org:80/WebAPI".
#' @param sourceCode             A single source code
#' @param sourceVocabularyId     The name of the source vocabulary id for the source code (e.g. ICD9CM, ICD10CM)
#' @param targetVocabularyId     The classification vocabulary id to map to
#' @param targetConceptClassId   (OPTIONAL) The concept class id of the classification term
#' @param relationshipId         The name of the concept relationship to filter by. Refer to the concept_relationship table for a list of options.
#' @param minDistance            The minimum hierarchy distance from the source code's standard concepts 
#'                               for the classification term. 
#'                               Default is 0 (meaning there is no minimum distance)
#' @param maxDistance            The maximum hierarchy distance from the source code's standard concepts 
#'                               for the classification term. 
#'                               Default is NULL (meaning there is no maximum distance)
#' 
#' @return A data frame summarizing the classification terms
#' 
#' @export
getClassificationFromSourceCode <- function(baseUrl, 
                                            sourceCode, 
                                            sourceVocabularyId,
                                            targetVocabularyId,
                                            targetConceptClassId = NULL,
                                            relationshipId = "Has ancestor of",
                                            minDistance = 0,
                                            maxDistance = NULL) {
  
  .checkBaseUrl(baseUrl)
  
  if (!is.null(maxDistance)) {
    if (minDistance > maxDistance) {
      stop("minDistance cannot be greater than maxDistance")
    }  
  }
  
  # obtain source concept id ------------------------------------
  sourceConcept <- getSourceConcept(baseUrl, sourceCode, sourceVocabularyId)
  
  if (length(sourceConcept) == 0) {
    return (data.frame())
  }
  
  # obtain standard concept(s) -------------------------------------
  standardConcepts <- getMappedStandardConcepts(baseUrl, sourceConcept$CONCEPT_ID)
  
  
  # get classification terms -----------------------
  
  classificationTerms <- lapply(standardConcepts, function(standardConcept) {
    relatedConcepts <- lapply(standardConcept$CONCEPT_ID, function(relatedConcept) {
      getRelatedConcepts(baseUrl, relatedConcept)
    })
    
    # get all of the matches within each set ------------------
    matchedTerms <- lapply(relatedConcepts, function(relatedConcept) {
      
      # get ancestors  ---------------------
      ancestors <- relatedConcept[sapply(relatedConcept, function(j) {
        if (!is.null(targetConceptClassId)) {
          j$VOCABULARY_ID == targetVocabularyId & j$CONCEPT_CLASS_ID == targetConceptClassId & 
            .hasRelationship(j$RELATIONSHIPS, relationshipId, minDistance, maxDistance)
        } else {
          j$VOCABULARY_ID == targetVocabularyId & 
            .hasRelationship(j$RELATIONSHIPS, relationshipId, minDistance, maxDistance)
        }
      })]
      
      results <- lapply(ancestors, function(ancestor) {
        distance <- data.frame(
          CONCEPT_ID = ancestor$CONCEPT_ID,
          CONCEPT_NAME = ancestor$CONCEPT_NAME,
          CONCEPT_CODE = ancestor$CONCEPT_CODE,
          CONCEPT_CLASS_ID = ancestor$CONCEPT_CLASS_ID,
          stringsAsFactors = FALSE
        )
      })
      result <- do.call(rbind.data.frame, results)
      
      if (nrow(result) > 0) {
        result$SOURCE_CONCEPT_ID <- sourceConcept$CONCEPT_ID
        result$SOURCE_CODE <- sourceCode
        result$SOURCE_CODE_NAME <- sourceConcept$CONCEPT_NAME
        result$SOURCE_VOCABULARY_ID <- sourceVocabularyId
        result$STANDARD_CONCEPT_ID <- standardConcept$CONCEPT_ID
        result$STANDARD_CODE <- standardConcept$CONCEPT_ID
        result$STANDARD_NAME <- standardConcept$CONCEPT_NAME
        
        dplyr::select(result,
                      SOURCE_CONCEPT_ID,
                      SOURCE_CODE,
                      SOURCE_CODE_NAME,
                      SOURCE_VOCABULARY_ID,
                      STANDARD_CONCEPT_ID = STANDARD_CONCEPT_ID,
                      STANDARD_CODE = STANDARD_CODE,
                      STANDARD_NAME = STANDARD_NAME,
                      CLASSIFICATION_CONCEPT_ID = CONCEPT_ID,
                      CLASSIFICATION_CODE = CONCEPT_CODE,
                      CLASSIFICATION_NAME = CONCEPT_NAME,
                      CONCEPT_CLASS_ID = CONCEPT_CLASS_ID)  
      } else {
        data.frame()
      }
    })
    do.call(rbind.data.frame, matchedTerms)
  })
  
  do.call(rbind.data.frame, classificationTerms)
  
}





