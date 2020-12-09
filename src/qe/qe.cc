#include "src/include/qe.h"

namespace PeterDB {
    RC getTargetAttributeValue(std::vector<Attribute> attrs, void *tupleBuffer, std::string lhsAttr, void *targetAttribute){
        unsigned numAttrs = attrs.size();
        unsigned nullIndicatorSize = ceil((double) numAttrs/8);
        char* nullIndicatorBuffer = new char[nullIndicatorSize];
        memcpy(nullIndicatorBuffer, tupleBuffer, nullIndicatorSize);
        unsigned attrCounter;
        unsigned attrOffset = nullIndicatorSize;
        for (attrCounter = 0; attrCounter < numAttrs; attrCounter++){
            if (attrs[attrCounter].name == lhsAttr){
                int byteIdx = attrCounter / 8;
                int bitIdx = attrCounter % 8;
                bool attrIsNull = nullIndicatorBuffer[byteIdx] & (int) 1 << (int) (7 - bitIdx);
                if (attrIsNull) {
                    free(nullIndicatorBuffer);
                    return -2;  // target attribute is null
                }
                else{
                    if (attrs[attrCounter].type == TypeVarChar){
                        unsigned varCharLen;
                        memcpy(&varCharLen, (char*) tupleBuffer + attrOffset, VC_LEN_SIZE);
                        memcpy(targetAttribute, &varCharLen, VC_LEN_SIZE);
                        memcpy((char*) targetAttribute + VC_LEN_SIZE, (char*) tupleBuffer + attrOffset + VC_LEN_SIZE, varCharLen);
                    }
                    else{
                        memcpy(targetAttribute, (char*) tupleBuffer + attrOffset, INT_OR_FLT_SIZE);
                    }
                }
                free(nullIndicatorBuffer);
                return 0;
            }
            else{
                if (attrs[attrCounter].type == TypeVarChar){
                    unsigned varCharLen;
                    memcpy(&varCharLen, (char*) tupleBuffer + attrOffset, VC_LEN_SIZE);
                    attrOffset += varCharLen + VC_LEN_SIZE;
                }
                else{
                    attrOffset += INT_OR_FLT_SIZE;
                }
            }
        }
        free(nullIndicatorBuffer);
        return -1; // lhsAttrName does not match any of the attribute names
    }

    void parseTuple(void *tupleBuffer, int &keyPtr, short &tupleLength, std::vector<Attribute> attrs, std::string conditionAttr) {
        unsigned numAttrs = attrs.size();
        unsigned nullIndicatorSize = ceil((double) numAttrs/8);
        char* nullIndicator = (char*) malloc(nullIndicatorSize);
        memcpy(nullIndicator, tupleBuffer, nullIndicatorSize);
        tupleLength = nullIndicatorSize;
        int counter = 0;
        int byteIdx;
        int bitIdx;
        for (Attribute attr : attrs){
            byteIdx = counter / 8;
            bitIdx = counter % 8;
            bool attrIsNull = nullIndicator[byteIdx] & (int) 1 << (int) (7 - bitIdx);
            if (attrIsNull) continue;
            if (attr.name == conditionAttr){
                keyPtr = tupleLength;
            }
            if (attr.type == TypeVarChar){
                unsigned varCharLen;
                memcpy(&varCharLen, (char*) tupleBuffer + tupleLength, VC_LEN_SIZE);
                tupleLength += varCharLen + VC_LEN_SIZE;
            }
            else{
                tupleLength += INT_OR_FLT_SIZE;
            }
            counter ++;
        }
        free(nullIndicator);
    }

    int getMaxTupleLength(std::vector<Attribute> attrs){
        unsigned nullIndicatorSize = ceil((double) attrs.size()/8);
        int maxTupleLength = nullIndicatorSize;
        for (Attribute attr : attrs){
            if (attr.type == TypeVarChar) maxTupleLength += VC_LEN_SIZE + attr.length;
            else maxTupleLength += INT_OR_FLT_SIZE;
        }
        return maxTupleLength;
    }

    void generateJoinedTuple(void* leftTuple, void* rightTuple, short leftTupleLength, short rightTupleLength,
                             std::vector<Attribute> leftAttrs, std::vector<Attribute> rightAttrs,  void* data){
        unsigned leftNullIndicatorSize = ceil((double) leftAttrs.size()/8);
        char* leftNullIndicator = (char*) malloc(leftNullIndicatorSize);
        memcpy(leftNullIndicator,(char*) leftTuple, leftNullIndicatorSize);

        unsigned rightNullIndicatorSize = ceil((double) rightAttrs.size()/8);
        char* rightNullIndicator = (char*) malloc(rightNullIndicatorSize);
        memcpy(rightNullIndicator,(char*) rightTuple, rightNullIndicatorSize);

        unsigned nullIndicatorSize = ceil((double) (leftAttrs.size() + rightAttrs.size())/8);
        char* nullIndicator = (char*) malloc(nullIndicatorSize);
        memset(nullIndicator, 0, nullIndicatorSize);
        int byteIdx;
        int bitIdx;
        for (int i = 0; i < nullIndicatorSize; i++){
            byteIdx = i / 8;
            bitIdx = i % 8;
            if (i < leftNullIndicatorSize){
                bool attrIsNull = leftNullIndicator[byteIdx] & (int) 1 << (int) (7 - bitIdx);
                if (attrIsNull) nullIndicator[byteIdx] += pow(2, 7-bitIdx);
            }
            else {
                int rightIdx = i - leftNullIndicatorSize;
                int rightByteIdx = rightIdx / 8;
                int rightBitIdx = rightIdx % 8;
                bool attrIsNull = rightNullIndicator[rightByteIdx] & (int) 1 << (int) (7 - rightBitIdx);
                if (attrIsNull) nullIndicator[byteIdx] += pow(2, 7-bitIdx);
            }
        }
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
        while (filterItr->getNextTuple(data) != RM_EOF){
            RC errCode = getTargetAttributeValue(attrs, data, condition.lhsAttr, targetAttrValue);
            if (errCode == -1){
                free(targetAttrValue);
                return -1;
            }
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
        return RM_EOF;
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

            unsigned valueVarCharLen;
            memcpy(&valueVarCharLen, (char*) condition.rhsValue.data, VC_LEN_SIZE);
            char* valueVarChar = (char*) malloc(valueVarCharLen);
            memcpy(valueVarChar, (char*) condition.rhsValue.data + VC_LEN_SIZE, valueVarCharLen);
            switch(condition.op) {
                case EQ_OP:
                    satisfied = std::string(valueVarChar, valueVarCharLen) == std::string(targetVarChar, targetVarCharLen);
                    break;
                case LT_OP:
                    satisfied = std::string(valueVarChar, valueVarCharLen) > std::string(targetVarChar, targetVarCharLen);
                    break;
                case LE_OP:
                    satisfied = std::string(valueVarChar, valueVarCharLen) >= std::string(targetVarChar, targetVarCharLen);
                    break;
                case GT_OP:
                    satisfied = std::string(valueVarChar, valueVarCharLen) < std::string(targetVarChar, targetVarCharLen);
                    break;
                case GE_OP:
                    satisfied = std::string(valueVarChar, valueVarCharLen) <= std::string(targetVarChar, targetVarCharLen);
                    break;
                case NE_OP:
                    satisfied = std::string(valueVarChar, valueVarCharLen) != std::string(targetVarChar, targetVarCharLen);
                    break;
            }
            free(targetVarChar);
            free(valueVarChar);
        }
            // Attribute is of type int
        else if (condition.rhsValue.type == TypeInt) {
            int check;
            memcpy(&check, (char*) targetAttrValue, INT_SIZE);
            int data;
            memcpy(&data, (char*) condition.rhsValue.data, INT_SIZE);
            switch(condition.op) {
                case EQ_OP:
                    satisfied = data == check;
                    break;
                case LT_OP:
                    satisfied = data > check;
                    break;
                case LE_OP:
                    satisfied = data >= check;
                    break;
                case GT_OP:
                    satisfied = data < check;
                    break;
                case GE_OP:
                    satisfied = data <= check;
                    break;
                case NE_OP:
                    satisfied = data != check;
                    break;
            }
        }
            // Attribute is of type real
        else {
            float check;
            memcpy(&check, (char*)targetAttrValue, FLT_SIZE);
            float data;
            memcpy(&data, (char*)condition.rhsValue.data, FLT_SIZE);
            switch(condition.op) {
                case EQ_OP:
                    satisfied = data == check;
                    break;
                case LT_OP:
                    satisfied = data > check;
                    break;
                case LE_OP:
                    satisfied = data >= check;
                    break;
                case GT_OP:
                    satisfied = data < check;
                    break;
                case GE_OP:
                    satisfied = data <= check;
                    break;
                case NE_OP:
                    satisfied = data != check;
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
        void* dataBuffer = malloc(getMaxTupleLength(attrs));
        while(projectItr->getNextTuple(dataBuffer) != RM_EOF){
            RC errCode = generateProjectAttrValues(data, dataBuffer);
            if (errCode != 0){
                free(dataBuffer);
                return errCode;
            }
            free(dataBuffer);
            return 0;
        }
        free(dataBuffer);
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

    RC Project::generateProjectAttrValues(void* data, void* dataBuffer){
        unsigned numAttrs = attrs.size();
        unsigned nullIndicatorSize = ceil((double) numAttrs/8);
        char* nullIndicatorBuffer = new char[nullIndicatorSize];
        memcpy(nullIndicatorBuffer, data, nullIndicatorSize);
        unsigned numProjectAttrs = attrNames.size();
        unsigned projectNullIndicatorSize = ceil((double) numProjectAttrs/8);
        char* projectNullIndicatorBuffer = new char[projectNullIndicatorSize];
        memset(projectNullIndicatorBuffer,0, projectNullIndicatorSize);
        int dataPtr = projectNullIndicatorSize;
        int dataBufferPtr ;
        int projectAttrCounter = 0;
        int attrCounter;
        int byteIdx;
        int bitIdx;
        int projectByteIdx;
        int projectBitIdx;
        bool attrIsNull;
        for (std::string attrName : attrNames){
            attrCounter = 0;
            dataBufferPtr = nullIndicatorSize;
            for (Attribute attr : attrs){
                if (attrName == attr.name){
                    byteIdx = attrCounter / 8;
                    bitIdx = attrCounter % 8;
                    attrIsNull = nullIndicatorBuffer[byteIdx] & (int) 1 << (int) (7 - bitIdx);
                    if (attrIsNull){
                        projectByteIdx = projectAttrCounter / 8;
                        projectBitIdx = attrCounter % 8;
                        projectNullIndicatorBuffer[projectByteIdx] += pow(2, 7-projectBitIdx);
                        break;
                    }
                    else{
                        if (attr.type == TypeVarChar){
                            unsigned varCharLen;
                            memcpy(&varCharLen, (char*) dataBuffer + dataBufferPtr, VC_LEN_SIZE);
                            memcpy((char*) data + dataPtr, &varCharLen, VC_LEN_SIZE);
                            memcpy((char*) data + dataPtr + VC_LEN_SIZE, (char*) dataBuffer + dataBufferPtr + VC_LEN_SIZE, varCharLen);
                            dataPtr += varCharLen + VC_LEN_SIZE;
                        }
                        else{
                            memcpy((char*) data + dataPtr, (char*) dataBuffer + dataBufferPtr, INT_OR_FLT_SIZE);
                            dataPtr += INT_OR_FLT_SIZE;
                        }
                        break;
                    }
                }
                else{
                    if (attr.type == TypeVarChar){
                        unsigned varCharLen;
                        memcpy(&varCharLen, (char*) dataBuffer + dataBufferPtr, VC_LEN_SIZE);
                        dataBufferPtr += varCharLen + VC_LEN_SIZE;
                    }
                    else {
                        dataBufferPtr += INT_OR_FLT_SIZE;
                    }
                }
                attrCounter ++;
            }
            if (attrCounter == attrs.size()){
                free(nullIndicatorBuffer);
                free(projectNullIndicatorBuffer);
                return -1;  // project attribute does not match any of the attributes
            }
            projectAttrCounter ++;
        }
        memcpy(data, projectNullIndicatorBuffer, projectNullIndicatorSize);
        free(nullIndicatorBuffer);
        free(projectNullIndicatorBuffer);
        return 0;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {
        this->leftIn = leftIn;
        this->rightIn = rightIn;
        this->condition = condition;
        this->numPages = numPages;
        this->leftIn->getAttributes(this->leftAttrs);
        this->rightIn->getAttributes(this->rightAttrs);
        this->isFirstGetNextTuple = true;
        this->block = malloc(numPages * PAGE_SIZE);
        this->leftTuple = malloc(getMaxTupleLength(this->leftAttrs));
        this->rightTuple = malloc(getMaxTupleLength(this->rightAttrs));
        this->isRM_EOF = false;
        this->counter = 0;
        for (Attribute leftAttr : this->leftAttrs){
            if (this->condition.lhsAttr == leftAttr.name){
                this->keyType = leftAttr.type;
                break;
            }
        }
    }

    BNLJoin::~BNLJoin() {
        free(block);
        free(leftTuple);
        free(rightTuple);
    }

    RC BNLJoin::getNextTuple(void *data) {
        int keyPtr;
        short rightTupleLength;
        if (isFirstGetNextTuple) {
            isFirstGetNextTuple = false;
            RC errCode = getNextBlockAndHash();
            if (errCode != 0) return QE_EOF;
            rightScan = rightIn->getNextTuple(rightTuple);
        }

        TupleRef leftTupleRef;
        while (rightScan != RM_EOF || getNextBlockAndHash() == 0){
            if (rightScan == RM_EOF){
                rightIn->setIterator();
                rightScan = rightIn->getNextTuple(rightTuple);
                counter = 0;
            }
            parseTuple(rightTuple, keyPtr, rightTupleLength, rightAttrs, condition.rhsAttr);
            if (keyType == TypeVarChar){
                unsigned keyVarCharLen;
                memcpy(&keyVarCharLen, (char*) rightTuple + keyPtr, VC_LEN_SIZE);
                char* keyVarChar = (char*) malloc(keyVarCharLen);
                memcpy(keyVarChar, (char*) rightTuple + keyPtr + VC_LEN_SIZE, keyVarCharLen);
                std::string varCharRightKey = std::string(keyVarChar, keyVarCharLen);
                free(keyVarChar);
                if (varCharHashTable.find(varCharRightKey) != varCharHashTable.end() && counter < varCharHashTable[varCharRightKey].size()){
                    leftTupleRef = varCharHashTable[varCharRightKey][counter];
                    counter ++;
                    memcpy(leftTuple, (char*) block + leftTupleRef.offset, leftTupleRef.length);
                }
                else {
                    rightScan = rightIn->getNextTuple(rightTuple);
                    counter = 0;
                    continue;
                }
            }
            else if (keyType == TypeInt) {
                int intRightKey;
                memcpy(&intRightKey, (char*) rightTuple + keyPtr, INT_SIZE);
                if (intHashTable.find(intRightKey) != intHashTable.end() && counter < intHashTable[intRightKey].size()){
                    leftTupleRef = intHashTable[intRightKey][counter];
                    counter ++;
                   // std::cout<<counter <<" "<< intHashTable[intRightKey].size()<<std::endl;
                    memcpy(leftTuple, (char*) block + leftTupleRef.offset, leftTupleRef.length);
                }
               else {
                   rightScan = rightIn->getNextTuple(rightTuple);
                   counter = 0;
                   continue;
               }
            }
            else  {
                float realRightKey;
                memcpy(&realRightKey, (char*) rightTuple + keyPtr, FLT_SIZE);
                if (realHashTable.find(realRightKey) != realHashTable.end() && counter < intHashTable[realRightKey].size()){
                    leftTupleRef = realHashTable[realRightKey][counter];
                    counter ++;
                    memcpy(leftTuple, (char* ) block + leftTupleRef.offset, leftTupleRef.length);
                }
                else {
                    rightScan = rightIn->getNextTuple(rightTuple);
                    counter = 0;
                    continue;
                }
            }
            generateJoinedTuple(leftTuple, rightTuple, leftTupleRef.length, rightTupleLength, leftAttrs, rightAttrs, data);
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

    RC BNLJoin::getNextBlockAndHash() {
        if (isRM_EOF){
            return RM_EOF;
        }
        int tupleOffset = 0;
        short tupleLength;
        int keyPtr;
        int maxTupleLength = getMaxTupleLength(leftAttrs);
        intHashTable.clear();
        realHashTable.clear();
        varCharHashTable.clear();
        while (leftIn->getNextTuple((char*) block + tupleOffset) != RM_EOF){
            parseTuple((char*) block + tupleOffset, keyPtr, tupleLength, leftAttrs, condition.lhsAttr);
            TupleRef tupleRef;
            tupleRef.offset = tupleOffset;
            tupleRef.length = tupleLength;
            if (keyType == TypeInt){
                int keyInt;
                memcpy(&keyInt, (char*) block + tupleOffset + keyPtr, INT_SIZE);
                if (intHashTable.find(keyInt) == intHashTable.end()){
                    intHashTable[keyInt] = std::vector<TupleRef>();
                }
                intHashTable[keyInt].push_back(tupleRef);
            }

            if (keyType == TypeReal){
                float keyFlt;
                memcpy(&keyFlt, (char*) block + tupleOffset + keyPtr, FLT_SIZE);
                if (realHashTable.find(keyFlt) == realHashTable.end()){
                    realHashTable[keyFlt] = std::vector<TupleRef>();
                }
                realHashTable[keyFlt].push_back(tupleRef);
            }

            if (keyType == TypeVarChar){
                unsigned varCharLen;
                memcpy(&varCharLen, (char*) block + tupleOffset + keyPtr, VC_LEN_SIZE);
                char* varChar = (char*) malloc(varCharLen);
                memcpy(varChar, (char*) block + tupleOffset + keyPtr + VC_LEN_SIZE, varCharLen);
                std::string keyVarChar = std::string(varChar, varCharLen);
                if (varCharHashTable.find(keyVarChar) == varCharHashTable.end()){
                    varCharHashTable[keyVarChar] = std::vector<TupleRef>();
                }
                varCharHashTable[keyVarChar].push_back(tupleRef);
                free(varChar);
            }
            if (tupleOffset + tupleLength > numPages * PAGE_SIZE - maxTupleLength) return 0;
            tupleOffset += tupleLength;
        }
        isRM_EOF = true;
        return 0;
    }


    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
        this->leftIn = leftIn;
        this->rightIn = rightIn;
        this->condition = condition;
        this->leftIn->getAttributes(this->leftAttrs);
        this->rightIn->getAttributes(this->rightAttrs);
        this->rightTuple = malloc(getMaxTupleLength(this->rightAttrs));
        this->leftTuple = malloc(getMaxTupleLength(this->leftAttrs));
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
        int dumKeyPtr;
        short rightTupleLength;
        short leftTupleLength;
        void* compareKey = malloc(getMaxTupleLength(leftAttrs));
        if (isFirstGetNextTuple){
            isFirstGetNextTuple = false;
            RC errCode = leftIn->getNextTuple(leftTuple);
            if (errCode != 0) return QE_EOF;
            errCode = getTargetAttributeValue(leftAttrs, leftTuple, condition.lhsAttr, compareKey);
            if (errCode != 0){
                free(compareKey);
                return errCode;
            }
            rightIn->setIterator(compareKey, compareKey,true,true);
        }

        RC rightScan = rightIn->getNextTuple(rightTuple);
        while(rightScan != IX_EOF || leftIn->getNextTuple(leftTuple) != -1){
            if (rightScan == IX_EOF){
                RC errCode = getTargetAttributeValue(leftAttrs, leftTuple, condition.lhsAttr, compareKey);
                if (errCode != 0){
                    free(compareKey);
                    return errCode;
                }
                rightIn->setIterator(compareKey, compareKey,true,true);
                rightScan = rightIn->getNextTuple(rightTuple);
                if (rightScan == IX_EOF) continue;
            }
            parseTuple(leftTuple, dumKeyPtr, leftTupleLength, leftAttrs, condition.lhsAttr);
            parseTuple(rightTuple, dumKeyPtr, rightTupleLength, rightAttrs, condition.rhsAttr);
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
        this->minVal.push_back(std::numeric_limits<float>::max());
        this->maxVal.push_back(std::numeric_limits<float>::min());
        this->sumVal.push_back(0);
        this->count.push_back(0);
        this->avgVal.push_back(0);
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {
        this->aggItr = input;
        this->aggAttr = aggAttr;
        this->op = op;
        this->aggItr->getAttributes(this->attrs);
        this->groupAttr = groupAttr;
        this->group = true;
        this->minVal.push_back(std::numeric_limits<float>::max());
        this->maxVal.push_back(std::numeric_limits<float>::min());
        this->sumVal.push_back(0);
        this->count.push_back(0);
        this->avgVal.push_back(0);
        this->groupCounter = 0;
        this->numGetNextTuple = 0;
        this->varCharGroupVector = std::vector<std::string>();
        this->intGroupVector = std::vector<int>();
        this->realGroupVector = std::vector<float>();
        this->isFirstGetNextTuple = true;
    }

    Aggregate::~Aggregate() {

    }

    RC Aggregate::getNextTuple(void *data) {
        if (!isFirstGetNextTuple && !group){
            return QE_EOF;
        }
        int maxTupleLength = getMaxTupleLength(attrs);
        void* tupleBuffer = malloc(maxTupleLength);
        void* aggAttributeBuffer = malloc(maxTupleLength);
        int nullIndicatorSize = 1;
        char nullIndicator = 0;  // 00000000
        memcpy((char*) data, &nullIndicator, nullIndicatorSize);
        if (!group) {
            isFirstGetNextTuple = false;

            while (aggItr->getNextTuple(tupleBuffer) != RM_EOF){
                RC errCode = getTargetAttributeValue(attrs, tupleBuffer, aggAttr.name, aggAttributeBuffer);
                if (errCode != 0){
                    free(tupleBuffer);
                    free(aggAttributeBuffer);
                    return errCode;
                }
                float attrVal = 0;
                if (aggAttr.type == TypeInt){
                    int intVal;
                    memcpy(&intVal, aggAttributeBuffer, INT_SIZE);
                    attrVal = (float) intVal;
                }
                else memcpy(&attrVal, aggAttributeBuffer, FLT_SIZE);

                minVal.back() = attrVal < minVal.back() ? attrVal : minVal.back();
                maxVal.back() = attrVal > maxVal.back() ? attrVal : maxVal.back();
                sumVal.back() += attrVal;
                count.back()++;
            }
            avgVal.back() = sumVal.back() / count.back();

            switch (op) {
                case MIN:
                    memcpy((char*) data + nullIndicatorSize, &minVal.back(), FLT_SIZE);
                    minVal.pop_back();
                    break;
                case MAX:
                    memcpy((char*) data + nullIndicatorSize, &maxVal.back(), FLT_SIZE);
                    maxVal.pop_back();
                    break;
                case SUM:
                    memcpy((char*) data + nullIndicatorSize, &sumVal.back(), FLT_SIZE);
                    sumVal.pop_back();
                    break;
                case AVG:
                    memcpy((char*) data + nullIndicatorSize, &avgVal.back(), FLT_SIZE);
                    avgVal.pop_back();
                    break;
                case COUNT:
                    memcpy((char*) data + nullIndicatorSize, &count.back(), FLT_SIZE);
                    count.pop_back();
                    break;
            }
        }
        else {
            numGetNextTuple++;
            if (isFirstGetNextTuple) {
                isFirstGetNextTuple = false;
                void* groupAttributeBuffer = malloc(getMaxTupleLength(attrs));
                while (aggItr->getNextTuple(tupleBuffer) != RM_EOF) {
                    RC errCode1 = getTargetAttributeValue(attrs, tupleBuffer, groupAttr.name, groupAttributeBuffer);
                    RC errCode2 = getTargetAttributeValue(attrs, tupleBuffer, aggAttr.name, aggAttributeBuffer);
                    if (errCode1 != 0 || errCode2 != 0) {
                        free(tupleBuffer);
                        free(groupAttributeBuffer);
                        free(aggAttributeBuffer);
                        return -1;
                    }
                    float aggAttrVal;
                    if (aggAttr.type == TypeInt) {
                        int intAggAttrVal;
                        memcpy(&intAggAttrVal, aggAttributeBuffer, INT_SIZE);
                        aggAttrVal = (float) intAggAttrVal;
                    }
                    else memcpy(&aggAttrVal, aggAttributeBuffer, FLT_SIZE);

                    if (groupAttr.type == TypeVarChar) {
                        unsigned varCharLen;
                        memcpy(&varCharLen, groupAttributeBuffer, VC_LEN_SIZE);
                        char* varChar = (char*) malloc(varCharLen);
                        memcpy(varChar, (char*) groupAttributeBuffer + VC_LEN_SIZE, varCharLen);
                        std::string groupVarChar = std::string(varChar, varCharLen);
                        if (varCharHashTable.find(groupVarChar) == varCharHashTable.end()) {
                            varCharHashTable[groupVarChar] = std::vector<float>();
                        }
                        varCharHashTable[groupVarChar].push_back(aggAttrVal);
                        free(varChar);
                    }
                    else if (groupAttr.type == TypeInt) {
                        int groupInt;
                        memcpy(&groupInt, groupAttributeBuffer, INT_SIZE);
                        if (intHashTable.find(groupInt) == intHashTable.end()) {
                            intHashTable[groupInt] = std::vector<float>();
                        }
                        intHashTable[groupInt].push_back(aggAttrVal);
                    }
                    else {
                        float groupFlt;
                        memcpy(&groupFlt, groupAttributeBuffer, FLT_SIZE);
                        if (realHashTable.find(groupFlt) == realHashTable.end()) {
                            realHashTable[groupFlt] = std::vector<float>();
                        }
                        realHashTable[groupFlt].push_back(aggAttrVal);
                    }
                }
                free(groupAttributeBuffer);
                if (groupAttr.type == TypeVarChar) {
                    for (auto pair : varCharHashTable) {
                        groupCounter++;
                        varCharGroupVector.push_back(pair.first);
                        for (float aggAttribute : pair.second) {
                            minVal.back() = aggAttribute < minVal.back() ? aggAttribute : minVal.back();
                            maxVal.back() = aggAttribute > maxVal.back() ? aggAttribute : maxVal.back();
                            sumVal.back() += aggAttribute;
                            count.back()++;
                        }
                        avgVal.back() = sumVal.back() / count.back();
                        minVal.push_back(std::numeric_limits<float>::max());
                        maxVal.push_back(std::numeric_limits<float>::min());
                        sumVal.push_back(0);
                        count.push_back(0);
                        avgVal.push_back(0);
                    }
                }
                else if (groupAttr.type == TypeInt) {
                    for (auto pair : intHashTable) {
                        groupCounter++;
                        intGroupVector.push_back(pair.first);
                        for (float aggAttribute : pair.second) {
                            minVal.back() = aggAttribute < minVal.back() ? aggAttribute : minVal.back();
                            maxVal.back() = aggAttribute > maxVal.back() ? aggAttribute : maxVal.back();
                            sumVal.back() += aggAttribute;
                            count.back()++;
                        }
                        avgVal.back() = sumVal.back() / count.back();
                        minVal.push_back(std::numeric_limits<float>::max());
                        maxVal.push_back(std::numeric_limits<float>::min());
                        sumVal.push_back(0);
                        count.push_back(0);
                        avgVal.push_back(0);
                    }
                }
                if (groupAttr.type == TypeReal) {
                    for (auto pair : realHashTable) {
                        groupCounter++;
                        realGroupVector.push_back(pair.first);
                        for (float aggAttribute : pair.second) {
                            minVal.back() = aggAttribute < minVal.back() ? aggAttribute : minVal.back();
                            maxVal.back() = aggAttribute > maxVal.back() ? aggAttribute : maxVal.back();
                            sumVal.back() += aggAttribute;
                            count.back()++;
                        }
                        avgVal.back() = sumVal.back() / count.back();
                        minVal.push_back(std::numeric_limits<float>::max());
                        maxVal.push_back(std::numeric_limits<float>::min());
                        sumVal.push_back(0);
                        count.push_back(0);
                        avgVal.push_back(0);
                    }
                }
            }
            int dataPtr = nullIndicatorSize;
            if (numGetNextTuple > groupCounter) return QE_EOF;
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
            switch (op) {
                case MIN:
                    minVal.pop_back();
                    memcpy((char*) data + dataPtr, &minVal.back(), FLT_SIZE);
                    break;
                case MAX:
                    maxVal.pop_back();
                    memcpy((char*) data + dataPtr, &maxVal.back(), FLT_SIZE);
                    break;
                case SUM:
                    sumVal.pop_back();
                    memcpy((char*) data + dataPtr, &sumVal.back(), FLT_SIZE);
                    break;
                case AVG:
                    avgVal.pop_back();
                    memcpy((char*) data + dataPtr, &avgVal.back(), FLT_SIZE);
                    break;
                case COUNT:
                    count.pop_back();
                    memcpy((char*) data + dataPtr, &count.back(), FLT_SIZE);
                    break;
            }
        }
        free(tupleBuffer);
        free(aggAttributeBuffer);
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
} // namespace PeterDB