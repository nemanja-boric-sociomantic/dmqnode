/******************************************************************************

    Ad4Max Analytics - DHT Client Connection Configuration

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        May 2010: initial release

    authors:        David Eckardt, Thomas Nicolai
    
    --
    
    Description:
    
    Reads a list of DHT client node items from an XML based configuration file.
    
    The DHT client node items list is an array of swarm.dht.DhtConst.NodeItem
    structures where each structure contains address, port and responsibility
    range of one DHT node to connect to.
    
    The DHT client configuration follows this scheme:
    
    $(D_CODE
    &lt;?xml version="1.0"?&gt;
    &lt;dhtnodes&gt;
        &lt;node&gt;
           &lt;address&gt;192.168.56.101&lt;/address&gt;
           &lt;port&gt;4711&lt;/port&gt;
           &lt;responsible&gt;
               &lt;min&gt;0x7&lt;/min&gt;
               &lt;max&gt;0xA&lt;/max&gt;
           &lt;/responsible&gt;
        &lt;/node&gt;
        &lt;node&gt;
           &lt;address&gt;dht1.example.org&lt;/address&gt;
           &lt;port&gt;4567&lt;/port&gt;
           &lt;!-- Using default responsibility range of [0x0 .. 0xF] --&gt;
        &lt;/node&gt;
    &lt;/dhtnodes&gt;
    )
    
    where
    
    $(UL 
        $(LI 'dhtnodes' must be the root element)
        $(LI 'dhtnodes' may contain an arbitrary number of 'node' elements or be
          empty, however, no 'node' won't make sense in most cases)
        $(LI 'node' must contain these elements, each one exactly once:
            $(UL
                $(LI 'address': address of this DHT node as a string)
                $(LI 'port':    connection port for this DHT node as an integer
                                number)
            )
        )
        $(LI 'node' may contain one 'responsible' element which must contain
             these elements, each one exactly once:
            $(UL
                $(LI 'min': minimum value of the responsibility range of this
                            node as an integer number)
                $(LI  'max': maximum value of the responsibility range of this
                             node as an integer number)
             )
         )
     )
         
     Integer numbers may be written either straight in decimal, or, with a
     '0x' prefix, in hexadecimal representation. For example,
     $(D_CODE &lt;port&gt;4567&lt;/port&gt;) has the same meaning as
     $(D_CODE &lt;port&gt;0x11D7&lt;/port&gt;).
     
     If the 'responsible' element is omitted, the responsibility range defaults
     to min = 0x0 and max = 0xF.

 ******************************************************************************/

module core.config.DhtNodesConfig;



/*******************************************************************************

    Imports

 ******************************************************************************/

private     import      swarm.dht.DhtConst;

private     import      tango.text.xml.Document;

private     import      tango.io.device.File;

private     import      Integer = tango.text.convert.Integer: toInt;

private     import      tango.stdc.ctype: tolower;



/*******************************************************************************

    Reads a list of DHT client node items from an XML based configuration file.
    
    The DHT client node items list is an array of swarm.dht.DhtConst.NodeItem
    structures where each structure contains address, port and responsibility
    range of one DHT node to connect to.
    
    The DHT client configuration follows this scheme:
    
    $(D_CODE
    &lt;?xml version="1.0"?&gt;
    &lt;dhtnodes&gt;
        &lt;node&gt;
           &lt;address&gt;192.168.56.101&lt;/address&gt;
           &lt;port&gt;4711&lt;/port&gt;
           &lt;responsible&gt;
               &lt;min&gt;0x7&lt;/min&gt;
               &lt;max&gt;0xA&lt;/max&gt;
           &lt;/responsible&gt;
        &lt;/node&gt;
        &lt;node&gt;
           &lt;address&gt;dht1.example.org&lt;/address&gt;
           &lt;port&gt;4567&lt;/port&gt;
           &lt;!-- Using default responsibility range of [0x0 .. 0xF] --&gt;
        &lt;/node&gt;
    &lt;/dhtnodes&gt;
    )
    
    where
    
    $(UL 
        $(LI 'dhtnodes' must be the root element)
        $(LI 'dhtnodes' may contain an arbitrary number of 'node' elements or be
          empty, however, no 'node' won't make sense in most cases)
        $(LI 'node' must contain these elements, each one exactly once:
            $(UL
                $(LI 'address': address of this DHT node as a string)
                $(LI 'port':    connection port for this DHT node as an integer
                                number)
            )
        )
        $(LI 'node' may contain one 'responsible' element which must contain
             these elements, each one exactly once:
            $(UL
                $(LI 'min': minimum value of the responsibility range of this
                            node as an integer number)
                $(LI  'max': maximum value of the responsibility range of this
                             node as an integer number)
             )
         )
     )
         
     Integer numbers may be written either straight in decimal, or, with a
     '0x' prefix, in hexadecimal representation. For example,
     $(D_CODE &lt;port&gt;4567&lt;/port&gt;) has the same meaning as
     $(D_CODE &lt;port&gt;0x11D7&lt;/port&gt;).
     
     If the 'responsible' element is omitted, the responsibility range defaults
     to min = 0x0 and max = 0xF.

 ******************************************************************************/



struct DhtNodesConfig
{
    /***************************************************************************
    
        XML element name string constants
    
     **************************************************************************/
    
    static:
    
    public const
    
        NODE_ID_ROOT        = "dhtnodes",
        NODE_ID_DHTNODE     = "node",
        
        NODE_ID_ADDRESS     = "address",
        NODE_ID_PORT        = "port",
        NODE_ID_RESPONSIBLE = "responsible",
        
        NODE_ID_RESPON_MIN  = "min",
        NODE_ID_RESPON_MAX  = "max";
        
    /***************************************************************************
    
        Aliases for Tango XML parser related types
    
     **************************************************************************/
    
    private alias Document!(char).Node Node;
    
    private alias XmlPath!(char).NodeSet NodeSet;
    
    /***************************************************************************
    
        Public methods
    
     **************************************************************************/
    
    /***************************************************************************
      
        Loads the templates from the template configuration file "filename".
     
        Params:
            filename = input file name
            templates = optional template base to add templates
    
        Returns:
            resulting template base
            
     **************************************************************************/
    
    public DhtConst.NodeItem[] readFile ( char[] filename )
    {
        char[] content;
        
        scope file = new File (filename);
        
        content.length = file.length();
        
        file.read(content);
        file.close();
        
        return read(content);
    }
    
    /***************************************************************************
     
        Parses "content" and loads the templates from it.

        Params:
            content = XML text content containing template configuration 
    
        Returns:
            resulting template base
     
     **************************************************************************/
    
    public DhtConst.NodeItem[] read ( char[] content )
    {
        DhtConst.NodeItem[] nodeitems;
        
        scope xml = new Document!(char);
        
        xml.parse(content);
        
        foreach (node; KeyNodeSet(xml.query()[this.NODE_ID_ROOT],
                       this.NODE_ID_DHTNODE).assertNodes())
        {
            nodeitems ~= readNodeItem(node);
        }
        
        return nodeitems;
    }
    
    /***************************************************************************
    
        Reads one DHT node item from configuration XML node node.
    
        Params:
            node = configuration XML node containing DHT node item
        
        Returns:
            resulting DHT node item
    
     **************************************************************************/
    
    private DhtConst.NodeItem readNodeItem ( Node node )
    {
        DhtConst.NodeItem nodeitem;
        
        NodeSet nodes = node.query();
        
        KeyNodeSet responsible = KeyNodeSet(nodes, this.NODE_ID_RESPONSIBLE);
        
        nodeitem.Address = KeyNodeSet(nodes, this.NODE_ID_ADDRESS).getSingleNode().value();
        
        nodeitem.Port = getInt(nodes, this.NODE_ID_PORT);
        
        if (responsible.nodeset.nodes.length)
        {
            nodeitem.MinValue = getInt(responsible.nodeset, this.NODE_ID_RESPON_MIN);
            nodeitem.MaxValue = getInt(responsible.nodeset, this.NODE_ID_RESPON_MAX);
        }
        
        return nodeitem;
    }
    
    /***************************************************************************
        
        Reads the numeric integer value of node named name of nodeset. If a "x"
        or "0x" value prefix is detected, the value is interpreted as
        hexadeximal. The value is read case-insensitively.
        
        Params:
            nodeset = node set containing a single node named name
            name = name of the node to read
            
        Returns:
            resulting value
    
     **************************************************************************/
    
    private int getInt ( NodeSet nodeset, char[] name )
    {
        char[] value = KeyNodeSet(nodeset, name).getSingleNode().value();
        
        return Integer.toInt(value, isHex(value)? 0x10 : 10);
    }
    
    /***************************************************************************
    
        Checks whether str has a "x" or "0x" value prefix, in a case-insensitive
        manner.
        
        Params:
            str = string to check for prefix
            
        Returns:
            true if such a prefix was detected or false otherwise
    
     **************************************************************************/
    
    private bool isHex ( char[] str )
    {
        if (str.length >= 2)
        {
            return (tolower(str[0]) == 'x') || ((str[0] == '0') && (tolower(str[1]) == 'x'));
        }
        
        return false;
    }
    
    /***************************************************************************
    
        KeyNodeSet structure
        
        Utility structure for convenient node set element assertion
    
     **************************************************************************/
    
    private struct KeyNodeSet
    {
        /***********************************************************************
             Error message template: MSG[0] is for "no" and MSG[1] for
             "more than one element".
         **********************************************************************/
        
        private static const char[][2][2] MSG =
        [
            ["missing '", "' element"],
            ["multiple '", "' elements"]
        ];
                
        /***********************************************************************
             
             NodeSet and regarding key
             
         **********************************************************************/
        
        NodeSet nodeset;
        char[] key;
             
        /***********************************************************************
        
            Creates a new instance with nodeset[key].
            
            Params:
                 nodeset = input nodeset
                 key     = nodeset query key
                 
            Returns:
                 new instance
             
         **********************************************************************/
        
        static KeyNodeSet opCall ( NodeSet nodeset, char[] key )
        {
            KeyNodeSet item;
            
            item.nodeset = nodeset[key].dup();
            item.key     = key;
            
            return item;
        }
        
        /***********************************************************************
         
           Iterator forwarding to nodeset
           
         **********************************************************************/
        
        int opApply ( int delegate ( ref Node ) dg )
        {
            return this.nodeset.opApply(dg);
        }
        
        /***********************************************************************
    
            Assert this.nodeset has at least one node.
            
            Params:
                 msg = message template, is concatenated to
                       "msg[0] ~ this.key ~ msg[1]"
                  
            Returns:
                 this instance
     
         **********************************************************************/
        
        KeyNodeSet assertNodes ( char[][2] msg = this.MSG[0] )
        {
            assert (this.nodeset.nodes.length, msg[0] ~ this.key ~ msg[1]);
            
            return *this;
        }
        
        /***********************************************************************
         
             Assert this.nodeset has exactly one node.
             
             Params:
                  msg = message template, is concatenated to
                        "msg[0][0] ~ this.key ~ msg[0][1]" if this.nodeset has
                        no nodes or to "msg[1][0] ~ this.key ~ msg[1][1]" if
                        this.nodeset has more than one node
                   
             Returns:
                  this instance
                  
         **********************************************************************/
        
        KeyNodeSet assertSingleNode ( char[][2][2] msg = this.MSG )
        {
            this.assertNodes(msg[0]);
            
            assert (this.nodeset.nodes.length == 1, msg[1][0] ~ this.key ~ msg[1][1]);
            
            return *this;
        }
        
        /***********************************************************************
        
            Assert this.nodeset has exactly one node and returns the node.
            
            Params:
                 msg = message template, is concatenated to
                       "msg[0][0] ~ this.key ~ msg[0][1]" if this.nodeset has
                       no nodes or to "msg[1][0] ~ this.key ~ msg[1][1]" if
                       this.nodeset has more than one node
                  
            Returns:
                 node of nodeset
                 
         **********************************************************************/
    
        Node getSingleNode ( char[][2][2] msg = this.MSG )
        {
            return this.assertSingleNode(msg).nodeset.nodes[0];
        }
    }
}