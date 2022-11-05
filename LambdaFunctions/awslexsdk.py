import boto3 as sdk
import ExperimentalSynonymFeatureForAmazonLex as syn

client=sdk.client('lexv2-models')
    
class SlotTypeUtils:
    
    slotID=''
    botID=''
    botVersion=''
    localeID=''
    
    def __init__(self,clientOJ,slotID,botID,botVersion,localeID):
        '''
        Costructor
            -Parameters:
                clientOJ:The SDK Object
                slotID:The id of the Amazon Lex V2 slot type.
                botID:The id of the Amazon Lex V2 bot. 
                botVersion:The version of the Amazon Lex V2 bot(Always DRAFT in developer mode).
                localeID:The id of language used in Amazon Lex V2. 
        '''
        
        self.clientOJ=clientOJ
        self.slotID=slotID
        self.botID=botID
        self.botVersion=botVersion
        self.localeID=localeID
        self.describeSlotType()
                
    def describeSlotType(self):
        '''
        This function describes a slotType using the atributes in the class,used for updating class.
        '''
        
        self.slotDescription = self.clientOJ.describe_slot_type(
            slotTypeId=self.slotID,
            botId=self.botID,
            botVersion=self.botVersion,
            localeId=self.localeID
        )
        
        
        


    def updateSlotType(self,arraySlotValues):
        '''
        This function acepts a list of posible values for a slot type 
        
        Return
            -Response: A json formated to a map in python with the response info.
        
        '''
        valuesSlotType=[]
        
        for slotValue in arraySlotValues:
            synObj=syn.wordTosynonym(slotValue)
            if(len(synObj)!=0):
                
                myValueMap={
                    'sampleValue':{
                        'value':slotValue
                    },
                    'synonyms':syn.wordTosynonym(slotValue)
                }
            else:
                
                 myValueMap={
                    'sampleValue':{
                        'value':slotValue
                    }
                }
                
                    
            valuesSlotType.append(myValueMap)
            
        response=self.clientOJ.update_slot_type(
            slotTypeId=self.slotID,
            slotTypeName=self.slotDescription['slotTypeName'],
            slotTypeValues=valuesSlotType,
            valueSelectionSetting=self.slotDescription['valueSelectionSetting'],
            botId=self.botID,
            botVersion=self.botVersion,
            localeId=self.localeID
        )
        
        return response
        
def updateSlotTypeAndBuildBot(clientOJ,slotID,botId_B,botVersion_B,localeID_B,sloteTypeValues):
    
    '''
    This function is uses to rebuild the bot with the updated changes
    '''
    
    mySlot=SlotTypeUtils(clientOJ,slotID,botId_B,botVersion_B,localeID_B)
    resp=mySlot.updateSlotType(sloteTypeValues)
    response = clientOJ.build_bot_locale(
    botId=botId_B,
    botVersion=botVersion_B,
    localeId=localeID_B
    )
    
    return response
    
    
def constructBot(array):
    
    response=updateSlotTypeAndBuildBot(client,'1ARCF9BNKO','3BXJYNJNTT','DRAFT','es_419',array) #Hay que esperar a que se contruya el bot 
    print(response)