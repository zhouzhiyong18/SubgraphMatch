#include <stdio.h>
#include <string.h>
#include <math.h>
#include <set>
#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name)subgraphMatch##name

struct vertexValue
{
    int type;
    int value;
    bool outputFlag = false;
};

struct edgeValue
{
    int type;
    int value;
};

struct messageValue
{
    unsigned long long toMatch;
    unsigned long long root;
};

struct subgraphOutEdge
{
    int type;
    int value;
    unsigned long long toId;
};

struct subgraphVertex
{
    unsigned long long id;
    int type;
    int value;
    int outEdgeNum = 0;
    subgraphOutEdge outEdgeArray[100];
};


subgraphVertex *subgraph;

vertexValue *vertexArray;

int subEdgeNum;

int leafNum = 0;

class VERTEX_CLASS_NAME(InputFormatter):public InputFormatter 
{
    //第一条记录，记录顶点个数，边的个数
 public:
    int64_t getVertexNum() 
    {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);
        m_total_vertex= n;
        return m_total_vertex;
    }
    int64_t getEdgeNum() 
    {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);
        m_total_edge= n;
        return m_total_edge;
    }
    //size有用吗？？
    int getVertexValueSize() 
    {
        m_n_value_size = sizeof(vertexValue);
        return m_n_value_size;
    }
    int getEdgeValueSize() 
    {
        m_e_value_size = sizeof(edgeValue);
        return m_e_value_size;
    }
    int getMessageValueSize() 
    {
        m_m_value_size = sizeof(messageValue);
        return m_m_value_size;
    }
    //Load graph architecture
    void loadGraph()   //要求记录中的from_ver是按顺序排列的  //没有出度？？
    {

        unsigned long long last_ver;
        unsigned long long from_ver;
        unsigned long long to_ver;
        
        edgeValue edg_val;
        vertexValue ver_val;

        int outdegree = 0;
        const char *line = getEdgeLine();

        sscanf(line,"%lld %lld %d %d",&from_ver,&to_ver,&edg_val.type,&edg_val.value);
        
        addEdge(from_ver,to_ver,&edg_val);

        last_ver = from_ver;
        outdegree++;

        for (int64_t j = 1; j < m_total_edge; j++) 
        {
            line= getEdgeLine();
            sscanf(line, "%lld %lld %d %d", &from_ver, &to_ver, &edg_val.type, &edg_val.value);
            if (last_ver != from_ver) 
            {
                //value=outdegree;
                addVertex(last_ver,&ver_val,outdegree);
                last_ver = from_ver;
                outdegree = 1;
            } 
            else 
            {
                outdegree++;
            }
            addEdge(from_ver,to_ver,&edg_val);
        }
          addVertex(last_ver,&ver_val,outdegree);  
        
    }
};

class VERTEX_CLASS_NAME(OutputFormatter):public OutputFormatter 
{
 public:
     //Result output function
     //Output when the degree of the vertex is not zero.
     //Vertices with degrees less than K are assigned zero.
    void writeResult() 
    {
        int64_t id;
        vertexValue value;
        char s[1024];    //只输出flag为true的点

        ResultIterator iter;
        while (!iter.done())
        {
            iter.getIdValue(id, &value);
            if(value.outputFlag){
                int n = sprintf(s, "%lld\n", (unsigned long long)id);
                writeNextResLine(s, n);
            }
            iter.next();
        }
    }
};

// An aggregator that records a int value tom compute sum.
class VERTEX_CLASS_NAME(Aggregator):public Aggregator<double> 
{
 public:
    void init() 
    {
        m_global = 0;
        m_local = 0;
    }
    void* getGlobal() 
    {
        return &m_global;
    }
    void setGlobal(const void*p)
    {
        m_global = *(double *)p;
    }
    void* getLocal() 
    {
        return &m_local;
    }
    void merge(const void*p) 
    {
        m_global += *(double *)p;
    }
    void accumulate(const void*p) 
    {
        m_local += *(double *)p;
    }
};

class VERTEX_CLASS_NAME():public Vertex<vertexValue, edgeValue, messageValue> 
{
 public:
    void compute(MessageIterator* pmsgs) 
    {
        
        if(getSuperstep() == 0)
        {
            //读数组，为顶点赋值
            unsigned long long id = getVertexId();

            vertexValue ver_val;
            ver_val.type = vertexArray[id].type;
            ver_val.value = vertexArray[id].value;
            * mutableValue() = ver_val;
        }
        else if(getSuperstep() == 1)
        {
            vertexValue ver_val = getValue();
            OutEdgeIterator iter = getOutEdgeIterator();
            if(iter.size() < subgraph[0].outEdgeNum || ver_val.type != subgraph[0].type || ver_val.value != subgraph[0].value);
            else if(subgraph[0].outEdgeNum == 0){
                ver_val.outputFlag = true;
                * mutableValue() = ver_val;
            }
            else{
                while (!iter.done()){
                    edgeValue edg_val = iter.getValue();
                    for(int i=0; i<subgraph[0].outEdgeNum; ++i){
                        if(edg_val.type == subgraph[0].outEdgeArray[i].type && edg_val.value == subgraph[0].outEdgeArray[i].value){
                            messageValue msg_val;
                            msg_val.toMatch = subgraph[0].outEdgeArray[i].toId;
                            msg_val.root = getVertexId();
                            sendMessageTo(iter.target(), msg_val);
                        }
                    }
                    iter.next();
                }
            }
            voteToHalt(); 
            return;
        }
        else if(getSuperstep() <= subEdgeNum+2){
            set<unsigned long long> leafFind;
            vertexValue ver_val = getValue();
            OutEdgeIterator iter = getOutEdgeIterator();
            for(; !pmsgs->done(); pmsgs->next()){
                messageValue pmsg_val = pmsgs->getValue();
                unsigned long long matchId = pmsg_val.toMatch;
                if(matchId == 0){
                    leafFind.insert(pmsg_val.root);
                    messageValue msg_val;
                    msg_val.toMatch = 0;
                    msg_val.root = pmsg_val.root;
                    sendMessageTo(getVertexId(), msg_val);
                }
                else if(iter.size() < subgraph[matchId].outEdgeNum || ver_val.type != subgraph[matchId].type || ver_val.value != subgraph[matchId].value);
                else if(subgraph[matchId].outEdgeNum == 0){

                    messageValue msg_val;
                    msg_val.toMatch = 0;
                    msg_val.root = matchId;
                    sendMessageTo(pmsg_val.root, msg_val);

                }
                else{
                    while (!iter.done()){
                        edgeValue edg_val = iter.getValue();
                        for(int i=0; i<subgraph[matchId].outEdgeNum; ++i){
                            if(edg_val.type == subgraph[matchId].outEdgeArray[i].type && edg_val.value == subgraph[matchId].outEdgeArray[i].value){
                                messageValue msg_val;
                                msg_val.toMatch = subgraph[matchId].outEdgeArray[i].toId;
                                msg_val.root = pmsg_val.root;
                                sendMessageTo(iter.target(), msg_val);
                            }
                        }
                        iter.next();
                    }
                }
            }
            if (leafFind.size() == leafNum){
                ver_val.outputFlag = true;
                * mutableValue() = ver_val;
            }
            voteToHalt(); 
            return;
        }
        else{
            voteToHalt(); 
            return;
        }               
    }
};

class VERTEX_CLASS_NAME(Graph):public Graph 
{
public:
    VERTEX_CLASS_NAME(Aggregator)* aggregator;

public:
    // argv[0]: KCore
    // argv[1]: The input path
    // argv[2]: The output path
    // argv[3]: Parameter K
    void init(int argc,char* argv[]) 
    {
        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        /*if (argc<=2) 
        {
           printf ("Usage: %s <input path> <output path> <K>\n", argv[0]);
           exit(1);
        }
*/
        m_pin_path = argv[1];

        char *vertexInput = argv[2];

        m_pout_path = argv[3];

        char *subInput = argv[4];

        int vertexNum;

        int subVertexNum;

        FILE *fp = NULL;
        fp = fopen(vertexInput, "r");
        fscanf(fp, "%d", &vertexNum);
        vertexArray = (vertexValue *)malloc(vertexNum*sizeof(vertexValue));
        for(int i=0; i<vertexNum; ++i){
            fscanf(fp, "%d %d", &vertexArray[i].type, &vertexArray[i].value);
        }
        fclose(fp);

        //读子图文件
        //subgraph = (subgraphVertex *)malloc(sub_vertex_num*sizeof(subgraphVertex));
        FILE *sub_fp = NULL;
        sub_fp = fopen(subInput, "r"); 

        fscanf(sub_fp, "%d", &subVertexNum);
        fscanf(sub_fp, "%d", &subEdgeNum);
        subgraph = (subgraphVertex *)malloc(subVertexNum*sizeof(subgraphVertex));

        for(int i=0; i<subVertexNum; i++){
            fscanf(sub_fp, "%lld %d %d", &subgraph[i].id, &subgraph[i].type ,&subgraph[i].value);
        }

        unsigned long long last_ver;
        unsigned long long from_ver;
        unsigned long long to_ver;
        int outdegree = 0;
        edgeValue edg_val;

        fscanf(sub_fp,"%lld %lld %d %d",&from_ver,&to_ver,&edg_val.type,&edg_val.value);
        last_ver = from_ver;
        outdegree++;
        subgraph[from_ver].outEdgeArray[0].type = edg_val.type;
        subgraph[from_ver].outEdgeArray[0].value = edg_val.value;
        subgraph[from_ver].outEdgeArray[0].toId = to_ver;

        for(int i=0; i<subEdgeNum; i++){
            fscanf(sub_fp, "%lld %lld %d %d", &from_ver, &to_ver, &edg_val.type, &edg_val.value);
            if (last_ver != from_ver) 
            {
                last_ver = from_ver;
                subgraph[last_ver].outEdgeNum = outdegree;
                outdegree = 1;
            } 
            else 
            {
                outdegree++;
            }
            subgraph[from_ver].outEdgeArray[outdegree-1].type = edg_val.type;
            subgraph[from_ver].outEdgeArray[outdegree-1].value = edg_val.value;
            subgraph[from_ver].outEdgeArray[outdegree-1].toId = to_ver;
        }

        fclose(sub_fp);

        /*
        //子图输出，验证结果
        for(int i =0;i<subVertexNum;i++)
        {
            printf("%lld %d %d %d",subgraph[i].id,subgraph[i].type,subgraph[i].value,subgraph[i].outEdgeNum);
            printf("\n");
           for(int j=0;j<subgraph[i].outEdgeNum;j++)
           {
               printf("%d %d %lld",subgraph[i].outEdgeArray[j].type,subgraph[i].outEdgeArray[j].value,subgraph[i].outEdgeArray[j].toId);
               printf("\n");
           }
        }
        */

        aggregator = new VERTEX_CLASS_NAME(Aggregator)[1];
        regNumAggr(1);
        regAggr(0,&aggregator[0]);
    }

    void term() 
    {
        delete[] aggregator;
    }
};

extern "C" Graph* create_graph() 
{
    Graph* pgraph = new VERTEX_CLASS_NAME(Graph);

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph* pobject) 
{
    delete ( VERTEX_CLASS_NAME()* )(pobject->m_pver_base);
    delete ( VERTEX_CLASS_NAME(OutputFormatter)* )(pobject->m_pout_formatter);
    delete ( VERTEX_CLASS_NAME(InputFormatter)* )(pobject->m_pin_formatter);
    delete ( VERTEX_CLASS_NAME(Graph)* )pobject;
}
