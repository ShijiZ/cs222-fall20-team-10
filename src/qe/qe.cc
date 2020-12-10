#include "src/include/qe.h"

namespace PeterDB {

    int getMaxTupleLength(std::vector<Attribute> attrs){
        unsigned nullIndicatorSize = ceil((double) attrs.size()/8);
        int maxTupleLength = nullIndicatorSize;
        for (Attribute attr : attrs){
            if (attr.type == TypeVarChar) maxTupleLength += VC_LEN_SIZE + attr.length;
            else maxTupleLength += INT_OR_FLT_SIZE;
        }
        return maxTupleLength;
    }

    RC getTargetAttrValue(std::vector<Attribute> attrs, void* tupleBuffer, std::string targetAttrName, void* targetAttrValue){
        unsigned numAttrs = attrs.size();
        unsigned nullIndicatorSize = ceil((double) numAttrs/8);
        char* nullIndicator = (char*) malloc(nullIndicatorSize);
        memcpy(nullIndicator, tupleBuffer, nullIndicatorSize);

        unsigned attrOffset = nullIndicatorSize;
        for (unsigned attrIdx = 0; attrIdx < numAttrs; attrIdx++){
            // target attribute found
            if (attrs[attrIdx].name == targetAttrName){
                int byteIdx = attrIdx / 8;
                int bitIdx = attrIdx % 8;
                bool attrIsNull = nullIndicator[byteIdx] & (int) 1 << (int) (7 - bitIdx);
                // target attribute is null
                if (attrIsNull) {
                    free(nullIndicator);
                    return -2;
                }
                // target attribute is not null, prepare targetAttrValue
                else{
                    if (attrs[attrIdx].type == TypeVarChar){
                        unsigned varCharLen;
                        memcpy(&varCharLen, (char*) tupleBuffer + attrOffset, VC_LEN_SIZE);
                        memcpy(targetAttrValue, &varCharLen, VC_LEN_SIZE);
                        memcpy((char*) targetAttrValue + VC_LEN_SIZE, (char*) tupleBuffer + attrOffset + VC_LEN_SIZE, varCharLen);
                    }
                    else{
                        memcpy(targetAttrValue, (char*) tupleBuffer + attrOffset, INT_OR_FLT_SIZE);
                    }
                }
                free(nullIndicator);
                return 0;
            }
            // target attribute has not been found, move attrOffset
            else{
                if (attrs[attrIdx].type == TypeVarChar){
                    unsigned varCharLen;
                    memcpy(&varCharLen, (char*) tupleBuffer + attrOffset, VC_LEN_SIZE);
                    attrOffset += varCharLen + VC_LEN_SIZE;
                }
                else{
                    attrOffset += INT_OR_FLT_SIZE;
                }
            }
        }
        free(nullIndicator);
        return -1; // targetAttrName does not match any of the attribute names
    }

    void parseTuple(void* tupleBuffer, std::vector<Attribute> attrs, std::string conditionAttr, int &keyOffset, short &tupleLength) {
        // read nullIndicator from tupleBuffer
        unsigned numAttrs = attrs.size();
        unsigned nullIndicatorSize = ceil((double) numAttrs/8);
        char* nullIndicator = (char*) malloc(nullIndicatorSize);
        memcpy(nullIndicator, tupleBuffer, nullIndicatorSize);

        tupleLength = nullIndicatorSize;
        int attrCounter = 0;
        for (Attribute attr : attrs){
            int byteIdx = attrCounter / 8;
            int bitIdx = attrCounter % 8;
            bool attrIsNull = nullIndicator[byteIdx] & (int) 1 << (int) (7 - bitIdx);
            if (attrIsNull) continue;

            // condition attribute found, set keyOffset
            if (attr.name == conditionAttr){
                keyOffset = tupleLength;
            }

            // accumulate tupleLength
            if (attr.type == TypeVarChar){
                unsigned varCharLen;
                memcpy(&varCharLen, (char*) tupleBuffer + tupleLength, VC_LEN_SIZE);
                tupleLength += varCharLen + VC_LEN_SIZE;
            }
            else{
                tupleLength += INT_OR_FLT_SIZE;
            }
            attrCounter++;
        }
        free(nullIndicator);
    }

    void generateJoinedTuple(void* leftTuple, void* rightTuple, short leftTupleLength, short rightTupleLength,
                             std::vector<Attribute> leftAttrs, std::vector<Attribute> rightAttrs, void* data){
        // get leftNullIndicator from left tuple
        unsigned leftNullIndicatorSize = ceil((double) leftAttrs.size()/8);
        char* leftNullIndicator = (char*) malloc(leftNullIndicatorSize);
        memcpy(leftNullIndicator,(char*) leftTuple, leftNullIndicatorSize);

        // get rightNullIndicator from right tuple
        unsigned rightNullIndicatorSize = ceil((double) rightAttrs.size()/8);
        char* rightNullIndicator = (char*) malloc(rightNullIndicatorSize);
        memcpy(rightNullIndicator,(char*) rightTuple, rightNullIndicatorSize);

        // initialize nullIndicator of the output data
        unsigned nullIndicatorSize = ceil((double) (leftAttrs.size() + rightAttrs.size())/8);
        char* nullIndicator = (char*) malloc(nullIndicatorSize);
        memset(nullIndicator, 0, nullIndicatorSize);

        for (int i = 0; i < nullIndicatorSize; i++){
            int byteIdx = i / 8;
            int bitIdx = i % 8;
            // generate left part nullIndicator based on leftNullIndicator
            if (i < leftNullIndicatorSize){
                bool attrIsNull = leftNullIndicator[byteIdx] & (int) 1 << (int) (7 - bitIdx);
                if (attrIsNull) nullIndicator[byteIdx] += pow(2, 7-bitIdx);
            }
            // generate right part nullIndicator based on rightNullIndicator
            else {
                int rightIdx = i - leftNullIndicatorSize;
                int rightByteIdx = rightIdx / 8;
                int rightBitIdx = rightIdx % 8;
                bool attrIsNull = rightNullIndicator[rightByteIdx] & (int) 1 << (int) (7 - rightBitIdx);
                if (attrIsNull) nullIndicator[byteIdx] += pow(2, 7-bitIdx);
            }
        }

        // copy nullIndicator, left tuple and right tuple (both without nullIndicator) into output data
        memcpy(data, nullIndicator, nullIndicatorSize);
        memcpy((char*) data + nullIndicatorSize,
               (char*) leftTuple + leftNullIndicatorSize,
               leftTupleLength - leftNullIndicatorSize);
        memcpy((char*) data + nullIndicatorSize + leftTupleLength - leftNullIndicatorSize,
               (char*) rightTuple + rightNullIndicatorSize,
               rightTupleLength - rightNullIndicatorSize);

        free(leftNullIndicator);
        free(rightNullIndicator);
        free(nullIndicator);
    }

    Filter::Filter(Iterator *input, const Condition &condition) {
        this->filterItr = input;
        this->condition = condition;
        input->getAttributes(this->attrs);
    }

    Filter::~Filter() {

    }

    RC Filter::getNextTuple(void *data) {
        void* targetAttrValue = malloc(getMaxTupleLength(attrs));
        while (filterItr->getNextTuple(data) != QE_EOF){
            RC errCode = getTargetAttrValue(attrs, data, condition.lhsAttr, targetAttrValue);
            if (errCode == -1){
                free(targetAttrValue);
                return -1;
            }
            // target attribute is null
            else if (errCode == -2) continue;
            else {
                bool satisfied = checkSatisfied(targetAttrValue);
                if (satisfied) {
                    free(targetAttrValue);
                    return 0;
                }
            }
        }
        free(targetAttrValue);
        return QE_EOF;
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = this->attrs;
        return 0;
    }

    bool Filter::checkSatisfied(void* targetAttrValue){
        bool satisfied = false;
        // Attribute is of type varChar
        if (condition.rhsValue.type == TypeVarChar) {
            unsigned targetVarCharLen;
            memcpy(&targetVarCharLen, (char*) targetAttrValue, VC_LEN_SIZE);
            char* targetVarChar = (char*) malloc(targetVarCharLen);
            memcpy(targetVarChar, (char*) targetAttrValue + VC_LEN_SIZE, targetVarCharLen);

            unsigned rhsValueVarCharLen;
            memcpy(&rhsValueVarCharLen, (char*) condition.rhsValue.data, VC_LEN_SIZE);
            char* rhsValueVarChar = (char*) malloc(rhsValueVarCharLen);
            memcpy(rhsValueVarChar, (char*) condition.rhsValue.data + VC_LEN_SIZE, rhsValueVarCharLen);
            switch(condition.op) {
                case EQ_OP:
                    satisfied = std::string(rhsValueVarChar, rhsValueVarCharLen) == std::string(targetVarChar, targetVarCharLen);
                    break;
                case LT_OP:
                    satisfied = std::string(rhsValueVarChar, rhsValueVarCharLen) > std::string(targetVarChar, targetVarCharLen);
                    break;
                case LE_OP:
                    satisfied = std::string(rhsValueVarChar, rhsValueVarCharLen) >= std::string(targetVarChar, targetVarCharLen);
                    break;
                case GT_OP:
                    satisfied = std::string(rhsValueVarChar, rhsValueVarCharLen) < std::string(targetVarChar, targetVarCharLen);
                    break;
                case GE_OP:
                    satisfied = std::string(rhsValueVarChar, rhsValueVarCharLen) <= std::string(targetVarChar, targetVarCharLen);
                    break;
                case NE_OP:
                    satisfied = std::string(rhsValueVarChar, rhsValueVarCharLen) != std::string(targetVarChar, targetVarCharLen);
                    break;
            }
            free(targetVarChar);
            free(rhsValueVarChar);
        }
        // Attribute is of type int
        else if (condition.rhsValue.type == TypeInt) {
            int targetInt;
            memcpy(&targetInt, (char*) targetAttrValue, INT_SIZE);
            int rhsInt;
            memcpy(&rhsInt, (char*) condition.rhsValue.data, INT_SIZE);
            switch(condition.op) {
                case EQ_OP:
                    satisfied = rhsInt == targetInt;
                    break;
                case LT_OP:
                    satisfied = rhsInt > targetInt;
                    break;
                case LE_OP:
                    satisfied = rhsInt >= targetInt;
                    break;
                case GT_OP:
                    satisfied = rhsInt < targetInt;
                    break;
                case GE_OP:
                    satisfied = rhsInt <= targetInt;
                    break;
                case NE_OP:
                    satisfied = rhsInt != targetInt;
                    break;
            }
        }
        // Attribute is of type real
        else {
            float targetFlt;
            memcpy(&targetFlt, (char*)targetAttrValue, FLT_SIZE);
            float rhsFlt;
            memcpy(&rhsFlt, (char*)condition.rhsValue.data, FLT_SIZE);
            switch(condition.op) {
                case EQ_OP:
                    satisfied = rhsFlt == targetFlt;
                    break;
                case LT_OP:
                    satisfied = rhsFlt > targetFlt;
                    break;
                case LE_OP:
                    satisfied = rhsFlt >= targetFlt;
                    break;
                case GT_OP:
                    satisfied = rhsFlt < targetFlt;
                    break;
                case GE_OP:
                    satisfied = rhsFlt <= targetFlt;
                    break;
                case NE_OP:
                    satisfied = rhsFlt != targetFlt;
                    break;
            }
        }
        return satisfied;
    }

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
        this->projectItr = input;
        this->attrNames = attrNames;
        input->getAttributes(this->attrs);
    }

    Project::~Project() {

    }

    RC Project::getNextTuple(void *data) {
        void* dataToBeProjected = malloc(getMaxTupleLength(attrs));
        while(projectItr->getNextTuple(dataToBeProjected) != RM_EOF){
            RC errCode = generateProjectTuple(data, dataToBeProjected);
            if (errCode != 0){
                free(dataToBeProjected);
                return errCode;
            }
            free(dataToBeProjected);
            return 0;
        }
        free(dataToBeProjected);
        return RM_EOF;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        for (std::string attrName : attrNames){
            for (Attribute attr : this->attrs){
                if (attrName == attr.name){
                    attrs.push_back(attr);
                    break;
                }
            }
        }
        return 0;
    }

    RC Project::generateProjectTuple(void* data, void* oldData){
        // prepare nullIndicator of dataToBeProjected
        unsigned numAttrs = attrs.size();
        unsigned oldNullIndicatorSize = ceil((double) numAttrs/8);
        char* oldNullIndicator = (char*) malloc(oldNullIndicatorSize);
        memcpy(oldNullIndicator, oldData, oldNullIndicatorSize);

        // initialize nullIndicator of projected data
        unsigned numProjectAttrs = attrNames.size();
        unsigned projectNullIndicatorSize = ceil((double) numProjectAttrs/8);
        char* projectNullIndicator = (char*) malloc(projectNullIndicatorSize);
        memset(projectNullIndicator,0, projectNullIndicatorSize);

        // prepare projected data
        int dataPtr = projectNullIndicatorSize;
        int projectAttrCounter = 0;
        // outer loop iterate through projected attribute names
        for (std::string attrName : attrNames) {
            int oldDataPtr = oldNullIndicatorSize;
            int oldAttrCounter = 0;
            // inner loop iterate through all attributes
            for (Attribute attr : attrs) {
                // one project attribute found
                if (attrName == attr.name) {
                    int byteIdx = oldAttrCounter / 8;
                    int bitIdx = oldAttrCounter % 8;
                    bool attrIsNull = oldNullIndicator[byteIdx] & (int) 1 << (int) (7 - bitIdx);
                    // project attribute is null, modify nullIndicator of projected data
                    if (attrIsNull) {
                        int projectByteIdx = projectAttrCounter / 8;
                        int projectBitIdx = projectAttrCounter % 8;
                        projectNullIndicator[projectByteIdx] += pow(2, 7-projectBitIdx);
                    }
                    // copy project attribute value into projected data
                    else {
                        if (attr.type == TypeVarChar) {
                            unsigned varCharLen;
                            memcpy(&varCharLen, (char*) oldData + oldDataPtr, VC_LEN_SIZE);
                            memcpy((char*) data + dataPtr, &varCharLen, VC_LEN_SIZE);
                            memcpy((char*) data + dataPtr + VC_LEN_SIZE, (char*) oldData + oldDataPtr + VC_LEN_SIZE, varCharLen);
                            dataPtr += varCharLen + VC_LEN_SIZE;
                        }
                        else {
                            memcpy((char*) data + dataPtr, (char*) oldData + oldDataPtr, INT_OR_FLT_SIZE);
                            dataPtr += INT_OR_FLT_SIZE;
                        }
                    }
                    break;
                }
                // this attribute will not be projected, move oldDataPtr
                else{
                    if (attr.type == TypeVarChar) {
                        unsigned varCharLen;
                        memcpy(&varCharLen, (char*) oldData + oldDataPtr, VC_LEN_SIZE);
                        oldDataPtr += varCharLen + VC_LEN_SIZE;
                    }
                    else {
                        oldDataPtr += INT_OR_FLT_SIZE;
                    }
                }
                oldAttrCounter++;
            }
            // project attribute does not match any of the attributes
            if (oldAttrCounter == attrs.size()) {
                free(oldNullIndicator);
                free(projectNullIndicator);
                return -1;
            }
            projectAttrCounter++;
        }
        // copy projectNullIndicator into projected data
        memcpy(data, projectNullIndicator, projectNullIndicatorSize);
        free(oldNullIndicator);
        free(projectNullIndicator);
        return 0;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {
        this->leftIn = leftIn;
        this->rightIn = rightIn;
        this->leftIn->getAttributes(this->leftAttrs);
        this->rightIn->getAttributes(this->rightAttrs);
        this->leftTuple = malloc(getMaxTupleLength(this->leftAttrs));
        this->rightTuple = malloc(getMaxTupleLength(this->rightAttrs));

        this->condition = condition;
        this->numPages = numPages;
        this->isFirstGetNextTuple = true;
        this->blockBuffer = malloc(numPages * PAGE_SIZE);
        this->leftScanEnded = false;
        this->tupleInfoCounter = 0;
        for (Attribute leftAttr : this->leftAttrs){
            if (this->condition.lhsAttr == leftAttr.name){
                this->keyType = leftAttr.type;
                break;
            }
        }
    }

    BNLJoin::~BNLJoin() {
        free(blockBuffer);
        free(leftTuple);
        free(rightTuple);
    }

    RC BNLJoin::getNextTuple(void *data) {
        // if the first time called, generate next block of left table and the first right tuple
        if (isFirstGetNextTuple) {
            isFirstGetNextTuple = false;
            RC errCode = generateNextBlockAndHash();
            if (errCode != 0) return QE_EOF;
            rightScanEnded = rightIn->getNextTuple(rightTuple);
        }

        // if right scan not ended, do not generate next block; otherwise, generate next block and reset iterator on right table
        while (rightScanEnded != RM_EOF || generateNextBlockAndHash() == 0){
            // if right scan ended, reset iterator on right table
            if (rightScanEnded == RM_EOF){
                rightIn->setIterator();
                rightScanEnded = rightIn->getNextTuple(rightTuple);
                tupleInfoCounter = 0;
            }
            // parse right tuple
            int keyOffset;
            short rightTupleLength;
            parseTuple(rightTuple, rightAttrs, condition.rhsAttr, keyOffset, rightTupleLength);

            TupleInfo leftTupleInfo;
            if (keyType == TypeVarChar) {
                unsigned keyVarCharLen;
                memcpy(&keyVarCharLen, (char*) rightTuple + keyOffset, VC_LEN_SIZE);
                char* keyVarChar = (char*) malloc(keyVarCharLen);
                memcpy(keyVarChar, (char*) rightTuple + keyOffset + VC_LEN_SIZE, keyVarCharLen);
                std::string varCharRightKey = std::string(keyVarChar, keyVarCharLen);
                free(keyVarChar);

                // if found matching left tuple and not all matching left tuples have been found,
                // get a left tuple on the block matched with the right tuple
                std::vector<TupleInfo> lhsTupleInfoVector = varCharHashTable[varCharRightKey];
                if (varCharHashTable.find(varCharRightKey) != varCharHashTable.end() && tupleInfoCounter < lhsTupleInfoVector.size()){
                    leftTupleInfo = lhsTupleInfoVector[tupleInfoCounter];
                    tupleInfoCounter++;
                    memcpy(leftTuple, (char*) blockBuffer + leftTupleInfo.offsetOnBlock, leftTupleInfo.length);
                }
                // if matching left tuple not found or all matching left tuples have been found, get next right tuple
                else {
                    rightScanEnded = rightIn->getNextTuple(rightTuple);
                    tupleInfoCounter = 0;
                    continue;
                }
            }
            else if (keyType == TypeInt) {
                int intRightKey;
                memcpy(&intRightKey, (char*) rightTuple + keyOffset, INT_SIZE);

                // if found matching left tuple and not all matching left tuples have been found,
                // get a left tuple on the block matched with the right tuple
                std::vector<TupleInfo> lhsTupleInfoVector = intHashTable[intRightKey];
                if (intHashTable.find(intRightKey) != intHashTable.end() && tupleInfoCounter < lhsTupleInfoVector.size()){
                    leftTupleInfo = lhsTupleInfoVector[tupleInfoCounter];
                    tupleInfoCounter++;
                    memcpy(leftTuple, (char*) blockBuffer + leftTupleInfo.offsetOnBlock, leftTupleInfo.length);
                }
                // if matching left tuple not found or all matching left tuples have been found, get next right tuple
                else {
                   rightScanEnded = rightIn->getNextTuple(rightTuple);
                   tupleInfoCounter = 0;
                   continue;
                }
            }
            else {
                float realRightKey;
                memcpy(&realRightKey, (char*) rightTuple + keyOffset, FLT_SIZE);

                // if found matching left tuple and not all matching left tuples have been found,
                // get a left tuple on the block matched with the right tuple
                std::vector<TupleInfo> lhsTupleInfoVector = realHashTable[realRightKey];
                if (realHashTable.find(realRightKey) != realHashTable.end() && tupleInfoCounter < lhsTupleInfoVector.size()){
                    leftTupleInfo = lhsTupleInfoVector[tupleInfoCounter];
                    tupleInfoCounter++;
                    memcpy(leftTuple, (char*) blockBuffer + leftTupleInfo.offsetOnBlock, leftTupleInfo.length);
                }
                // if matching left tuple not found or all matching left tuples have been found, get next right tuple
                else {
                    rightScanEnded = rightIn->getNextTuple(rightTuple);
                    tupleInfoCounter = 0;
                    continue;
                }
            }

            generateJoinedTuple(leftTuple, rightTuple, leftTupleInfo.length, rightTupleLength, leftAttrs, rightAttrs, data);
            return 0;
        }
        return QE_EOF;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        for (Attribute leftAttr : leftAttrs) attrs.push_back(leftAttr);
        for (Attribute rightAttr : rightAttrs) attrs.push_back(rightAttr);
        return 0;
    }

    RC BNLJoin::generateNextBlockAndHash() {
        if (leftScanEnded) return RM_EOF;
        int offsetOnBlock = 0;
        int maxTupleLength = getMaxTupleLength(leftAttrs);
        intHashTable.clear();
        realHashTable.clear();
        varCharHashTable.clear();
        while (leftIn->getNextTuple((char*) blockBuffer + offsetOnBlock) != RM_EOF){
            // parse tuple and build tupleInfo
            int keyOffset;
            short tupleLength;
            parseTuple((char*) blockBuffer + offsetOnBlock, leftAttrs, condition.lhsAttr, keyOffset, tupleLength);
            TupleInfo tupleInfo;
            tupleInfo.offsetOnBlock = offsetOnBlock;
            tupleInfo.length = tupleLength;

            if (keyType == TypeVarChar) {
                unsigned varCharLen;
                memcpy(&varCharLen, (char*) blockBuffer + offsetOnBlock + keyOffset, VC_LEN_SIZE);
                char* varChar = (char*) malloc(varCharLen);
                memcpy(varChar, (char*) blockBuffer + offsetOnBlock + keyOffset + VC_LEN_SIZE, varCharLen);
                std::string keyVarChar = std::string(varChar, varCharLen);
                free(varChar);

                // keyVarChar not found, insert new key-value into hash table
                if (varCharHashTable.find(keyVarChar) == varCharHashTable.end()){
                    varCharHashTable[keyVarChar] = std::vector<TupleInfo>();
                }
                // insert tupleInfo into mapped vector
                varCharHashTable[keyVarChar].push_back(tupleInfo);
            }
            else if (keyType == TypeInt) {
                int keyInt;
                memcpy(&keyInt, (char*) blockBuffer + offsetOnBlock + keyOffset, INT_SIZE);

                // keyInt not found, insert new key-value into hash table
                if (intHashTable.find(keyInt) == intHashTable.end()){
                    intHashTable[keyInt] = std::vector<TupleInfo>();
                }
                // insert tupleInfo into mapped vector
                intHashTable[keyInt].push_back(tupleInfo);
            }
            else {
                float keyFlt;
                memcpy(&keyFlt, (char*) blockBuffer + offsetOnBlock + keyOffset, FLT_SIZE);

                // keyFlt not found, insert new key-value into hash table
                if (realHashTable.find(keyFlt) == realHashTable.end()){
                    realHashTable[keyFlt] = std::vector<TupleInfo>();
                }
                // insert tupleInfo into mapped vector
                realHashTable[keyFlt].push_back(tupleInfo);
            }

            // if block is full, return 0
            if (offsetOnBlock + tupleLength > numPages * PAGE_SIZE - maxTupleLength) return 0;
            offsetOnBlock += tupleLength;
        }
        leftScanEnded = true;
        return 0;
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
        this->leftIn = leftIn;
        this->rightIn = rightIn;
        this->leftIn->getAttributes(this->leftAttrs);
        this->rightIn->getAttributes(this->rightAttrs);
        this->leftTuple = malloc(getMaxTupleLength(this->leftAttrs));
        this->rightTuple = malloc(getMaxTupleLength(this->rightAttrs));

        this->condition = condition;
        this->isFirstGetNextTuple = true;
        for (Attribute leftAttr : this->leftAttrs){
            if (this->condition.lhsAttr == leftAttr.name){
                this->keyType = leftAttr.type;
            }
        }
    }

    INLJoin::~INLJoin() {
        free(leftTuple);
        free(rightTuple);
    }

    RC INLJoin::getNextTuple(void *data) {
        void* compareKey = malloc(getMaxTupleLength(leftAttrs));

        // if the first time called, get one left tuple, generate compareKey and set iterator on right table
        if (isFirstGetNextTuple) {
            isFirstGetNextTuple = false;
            RC errCode = leftIn->getNextTuple(leftTuple);
            if (errCode != 0) return QE_EOF;
            errCode = getTargetAttrValue(leftAttrs, leftTuple, condition.lhsAttr, compareKey);
            if (errCode != 0){
                free(compareKey);
                return errCode;
            }
            rightIn->setIterator(compareKey, compareKey,true,true);
        }

        // get one matching right tuple
        RC rightScanEnded = rightIn->getNextTuple(rightTuple);

        // if right scan not ended, do not get next left tuple; otherwise, get next left tuple, reset iterator on right table, and get a new matching right tuple
        while (rightScanEnded != IX_EOF || leftIn->getNextTuple(leftTuple) != QE_EOF){
            if (rightScanEnded == IX_EOF){
                RC errCode = getTargetAttrValue(leftAttrs, leftTuple, condition.lhsAttr, compareKey);
                if (errCode != 0){
                    free(compareKey);
                    return errCode;
                }
                rightIn->setIterator(compareKey, compareKey,true,true);
                rightScanEnded = rightIn->getNextTuple(rightTuple);

                // no matching right tuple exist for current left tuple
                if (rightScanEnded == IX_EOF) continue;
            }

            // parse left and right tuples, then join together, return 0
            int dumKeyPtr;
            short leftTupleLength;
            short rightTupleLength;
            parseTuple(leftTuple, leftAttrs, condition.lhsAttr, dumKeyPtr, leftTupleLength);
            parseTuple(rightTuple, rightAttrs, condition.rhsAttr, dumKeyPtr, rightTupleLength);

            generateJoinedTuple(leftTuple, rightTuple, leftTupleLength, rightTupleLength, leftAttrs, rightAttrs, data);
            return 0;
        }
        free(compareKey);
        return QE_EOF;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        for (Attribute leftAttr : leftAttrs) attrs.push_back(leftAttr);
        for (Attribute rightAttr : rightAttrs) attrs.push_back(rightAttr);
        return 0;
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {

    }

    GHJoin::~GHJoin() {

    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {
        this->aggItr = input;
        this->aggAttr = aggAttr;
        this->op = op;
        this->aggItr->getAttributes(this->attrs);
        this->isFirstGetNextTuple = true;
        this->group = false;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {
        this->aggItr = input;
        this->aggAttr = aggAttr;
        this->op = op;
        this->aggItr->getAttributes(this->attrs);
        this->isFirstGetNextTuple = true;
        this->groupAttr = groupAttr;
        this->group = true;

        this->groupCounter = 0;
        this->numGetNextTuple = 0;
        this->varCharGroupVector = std::vector<std::string>();
        this->intGroupVector = std::vector<int>();
        this->realGroupVector = std::vector<float>();
    }

    Aggregate::~Aggregate() {

    }

    RC Aggregate::getNextTuple(void *data) {
        if (!isFirstGetNextTuple && !group) return QE_EOF;

        int maxTupleLength = getMaxTupleLength(attrs);
        void* tupleBuffer = malloc(maxTupleLength);
        void* aggAttrBuffer = malloc(maxTupleLength);

        // prepare nullIndicator
        int nullIndicatorSize = 1;
        char nullIndicator = 0;  // 00000000
        memcpy((char*) data, &nullIndicator, nullIndicatorSize);

        // not groupBy
        if (!group) {
            isFirstGetNextTuple = false;
            initRunningInfoVectors();

            // iterate through the table and maintain the running information
            while (aggItr->getNextTuple(tupleBuffer) != RM_EOF){
                RC errCode = getTargetAttrValue(attrs, tupleBuffer, aggAttr.name, aggAttrBuffer);
                if (errCode != 0){
                    free(tupleBuffer);
                    free(aggAttrBuffer);
                    return errCode;
                }
                float attrVal = 0;
                if (aggAttr.type == TypeInt){
                    int intVal;
                    memcpy(&intVal, aggAttrBuffer, INT_SIZE);
                    attrVal = (float) intVal;
                }
                else memcpy(&attrVal, aggAttrBuffer, FLT_SIZE);

                minVal.back() = attrVal < minVal.back() ? attrVal : minVal.back();
                maxVal.back() = attrVal > maxVal.back() ? attrVal : maxVal.back();
                sumVal.back() += attrVal;
                count.back()++;
            }
            avgVal.back() = sumVal.back() / count.back();

            // output running information based on op
            switch (op) {
                case MIN:
                    memcpy((char*) data + nullIndicatorSize, &minVal.back(), FLT_SIZE);
                    break;
                case MAX:
                    memcpy((char*) data + nullIndicatorSize, &maxVal.back(), FLT_SIZE);
                    break;
                case SUM:
                    memcpy((char*) data + nullIndicatorSize, &sumVal.back(), FLT_SIZE);
                    break;
                case AVG:
                    memcpy((char*) data + nullIndicatorSize, &avgVal.back(), FLT_SIZE);
                    break;
                case COUNT:
                    memcpy((char*) data + nullIndicatorSize, &count.back(), FLT_SIZE);
                    break;
            }
        }
        // groupBy
        else {
            numGetNextTuple++;
            // if called the first time, build the hash table that a key is an attribute value of
            // the groupAttr, and the value is a vector of the aggAttr values in that group
            if (isFirstGetNextTuple) {
                isFirstGetNextTuple = false;
                void* groupAttrBuffer = malloc(getMaxTupleLength(attrs));
                while (aggItr->getNextTuple(tupleBuffer) != RM_EOF) {
                    // get attribute value of both groupAttr and aggAttr
                    RC errCode1 = getTargetAttrValue(attrs, tupleBuffer, groupAttr.name, groupAttrBuffer);
                    RC errCode2 = getTargetAttrValue(attrs, tupleBuffer, aggAttr.name, aggAttrBuffer);
                    if (errCode1 != 0 || errCode2 != 0) {
                        free(tupleBuffer);
                        free(groupAttrBuffer);
                        free(aggAttrBuffer);
                        return -1;
                    }

                    // prepare aggAttrVal
                    float aggAttrVal;
                    if (aggAttr.type == TypeInt) {
                        int intAggAttrVal;
                        memcpy(&intAggAttrVal, aggAttrBuffer, INT_SIZE);
                        aggAttrVal = (float) intAggAttrVal;
                    }
                    else memcpy(&aggAttrVal, aggAttrBuffer, FLT_SIZE);

                    if (groupAttr.type == TypeVarChar) {
                        unsigned varCharLen;
                        memcpy(&varCharLen, groupAttrBuffer, VC_LEN_SIZE);
                        char* varChar = (char*) malloc(varCharLen);
                        memcpy(varChar, (char*) groupAttrBuffer + VC_LEN_SIZE, varCharLen);
                        std::string groupVarChar = std::string(varChar, varCharLen);
                        free(varChar);

                        // groupVarChar not found, insert new key-value into hash table
                        if (varCharHashTable.find(groupVarChar) == varCharHashTable.end()) {
                            varCharHashTable[groupVarChar] = std::vector<float>();
                        }
                        // insert aggAttrVal into mapped vector
                        varCharHashTable[groupVarChar].push_back(aggAttrVal);
                    }
                    else if (groupAttr.type == TypeInt) {
                        int groupInt;
                        memcpy(&groupInt, groupAttrBuffer, INT_SIZE);

                        // groupInt not found, insert new key-value into hash table
                        if (intHashTable.find(groupInt) == intHashTable.end()) {
                            intHashTable[groupInt] = std::vector<float>();
                        }
                        // insert aggAttrVal into mapped vector
                        intHashTable[groupInt].push_back(aggAttrVal);
                    }
                    else {
                        float groupFlt;
                        memcpy(&groupFlt, groupAttrBuffer, FLT_SIZE);

                        // groupFlt not found, insert new key-value into hash table
                        if (realHashTable.find(groupFlt) == realHashTable.end()) {
                            realHashTable[groupFlt] = std::vector<float>();
                        }
                        // insert aggAttrVal into mapped vector
                        realHashTable[groupFlt].push_back(aggAttrVal);
                    }
                }
                free(groupAttrBuffer);

                if (groupAttr.type == TypeVarChar) {
                    // calculate running information for every group
                    for (auto pair : varCharHashTable) {
                        groupCounter++;
                        varCharGroupVector.push_back(pair.first);
                        initRunningInfoVectors();
                        // calculate running information for one group
                        for (float aggAttribute : pair.second) {
                            minVal.back() = aggAttribute < minVal.back() ? aggAttribute : minVal.back();
                            maxVal.back() = aggAttribute > maxVal.back() ? aggAttribute : maxVal.back();
                            sumVal.back() += aggAttribute;
                            count.back()++;
                        }
                        avgVal.back() = sumVal.back() / count.back();
                    }
                }
                else if (groupAttr.type == TypeInt) {
                    // calculate running information for every group
                    for (auto pair : intHashTable) {
                        groupCounter++;
                        intGroupVector.push_back(pair.first);
                        initRunningInfoVectors();
                        // calculate running information for one group
                        for (float aggAttribute : pair.second) {
                            minVal.back() = aggAttribute < minVal.back() ? aggAttribute : minVal.back();
                            maxVal.back() = aggAttribute > maxVal.back() ? aggAttribute : maxVal.back();
                            sumVal.back() += aggAttribute;
                            count.back()++;
                        }
                        avgVal.back() = sumVal.back() / count.back();
                    }
                }
                if (groupAttr.type == TypeReal) {
                    // calculate running information for every group
                    for (auto pair : realHashTable) {
                        groupCounter++;
                        realGroupVector.push_back(pair.first);
                        initRunningInfoVectors();
                        // calculate running information for one group
                        for (float aggAttribute : pair.second) {
                            minVal.back() = aggAttribute < minVal.back() ? aggAttribute : minVal.back();
                            maxVal.back() = aggAttribute > maxVal.back() ? aggAttribute : maxVal.back();
                            sumVal.back() += aggAttribute;
                            count.back()++;
                        }
                        avgVal.back() = sumVal.back() / count.back();
                    }
                }
            }
            // the running information of all groups has been returned
            if (numGetNextTuple > groupCounter) return QE_EOF;

            // prepare groupAttr for output data
            int dataPtr = nullIndicatorSize;
            if (groupAttr.type == TypeVarChar){
                unsigned varCharLen = varCharGroupVector.back().size();
                memcpy((char*) data + dataPtr, &varCharLen, VC_LEN_SIZE);
                memcpy((char*) data + dataPtr + VC_LEN_SIZE, &varCharGroupVector.back(), varCharLen);
                dataPtr += varCharLen + VC_LEN_SIZE;
                varCharGroupVector.pop_back();
            }
            else if (groupAttr.type == TypeInt){
                memcpy((char*) data + dataPtr, &intGroupVector.back(), INT_SIZE);
                dataPtr += INT_SIZE;
                intGroupVector.pop_back();
            }
            else {
                memcpy((char*) data + dataPtr, &realGroupVector.back(), FLT_SIZE);
                dataPtr += FLT_SIZE;
                realGroupVector.pop_back();
            }

            // prepare running information for output data
            switch (op) {
                case MIN:
                    memcpy((char*) data + dataPtr, &minVal.back(), FLT_SIZE);
                    minVal.pop_back();
                    break;
                case MAX:
                    memcpy((char*) data + dataPtr, &maxVal.back(), FLT_SIZE);
                    maxVal.pop_back();
                    break;
                case SUM:
                    memcpy((char*) data + dataPtr, &sumVal.back(), FLT_SIZE);
                    sumVal.pop_back();
                    break;
                case AVG:
                    memcpy((char*) data + dataPtr, &avgVal.back(), FLT_SIZE);
                    avgVal.pop_back();
                    break;
                case COUNT:
                    memcpy((char*) data + dataPtr, &count.back(), FLT_SIZE);
                    count.pop_back();
                    break;
            }
        }
        free(tupleBuffer);
        free(aggAttrBuffer);
        return 0;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        std::string opName;
        switch (op){
            case MIN:
                opName = "MIN";
                break;
            case MAX:
                opName = "MAX";
                break;
            case SUM:
                opName = "SUM";
                break;
            case AVG:
                opName = "AVG";
                break;
            case COUNT:
                opName = "COUNT";
                break;
        }
        Attribute attr = aggAttr;
        attr.type = TypeReal;
        attr.name = opName + '(' + attr.name + ')';
        if (group) attrs.push_back(groupAttr);
        attrs.push_back(attr);

        return 0;
    }

    void Aggregate::initRunningInfoVectors() {
        minVal.push_back(std::numeric_limits<float>::max());
        maxVal.push_back(std::numeric_limits<float>::min());
        sumVal.push_back(0);
        count.push_back(0);
        avgVal.push_back(0);
    }
} // namespace PeterDB