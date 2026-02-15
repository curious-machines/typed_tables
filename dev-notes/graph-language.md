  ```
  I'd like to start a discussion for a revamped version of the graph command. We will not implement this yet, but I want to capture all of the         
  points and decisions of this discussion so that we'll have all of that available when we're ready to implement the idea.                            
                                                                                                                                                       
  We have made good progress on improving the syntax for views; however, I think that we can build a more flexible system that gives the user more     
   control as to what is being displayed. We can start with simple selections for the graph and then expand and revise from there. It would be         
  really helpful if you keep in mind what functionality we have already so that we can, at the very least, recreate that functionality, hopefully      
  going much further.                                                                                                                                  
                                                                                                                                                       
  The idea I have now is a little bit inspired by xpath. The syntax won't look the same, but the semantics are similar. For our first round of         
  graphs, we would show composites, interfaces, enums, aliases, dictionaries, and arrays; all of our container types. The idea is that we are          
  building a type of expression that is maintaining a set of nodes and edges. These nodes and edges should hold enough information to build the        
  graphs we already support.                                                                                                                           
                                                                                                                                                       
  We use an expression to process this set. Initially, the set is empty. The first step in the expression will choose elements to work with. For       
  example, to select all composites, we could say "graph composites". The set now contains all composites. We could end here, showing a graph of a     
   composite forest; no fields, no interfaces, etc., etc. Or, our expression can continue, further processing the current result set.                  
                                                                                                                                                       
  If we want to process a single composite, we could say "graph composites{name=Person}". If we want to process two composites, we could say           
  "graph composite{name=Person|Root}". If we want to process a composite and an interface (something we can't do currently), then we could say         
  "graph {composite{name=Person}, interface{name=Sizeable}}. Note the use of set notation to build a comma-delimited list of nodes to add in this      
  step of the expression. Unfornately, this break our ability to attach metadata to "graph", but perhaps, we can work around that later. That          
  asepct is not as important as the graph expression language we are building.                                                                         
                                                                                                                                                       
  Now, the next step in the expression evaluation can either completely replace the current result set, or append to it. For example, if I want a      
  composite and all of its immediately implemented interfaces (and not those interface's ancestors), I might say something like "graph                 
  composite(name=Person) + .interfaces". The plus sign indicates that we want to preserve the current result set and then add the result of the        
  next operation as it is applied to all of the current set. What's not clear with this syntax is how do we know that the edges from Person to its     
   interfaces will be included in the result set? But, to the example, ".interfaces" acts like a property on composites (and probably interfaces       
  as well). "interfaces" is part of the composite's ancester axis/chain, specifically the "implements" ancestor axis. Composites also have an          
  "extends" ancestor axis that we can access via .extends. Now, we may want a specific ancestor "graph composite{name=Person} +                        
  .interfaces{name=Sizeable}, or we may want all interfaces from the axis "graph composite{name=Person} + .interfaces{name=*, ancestors=true}".        
  Another axis exists for fields. Some of these axes are availabe on both composite and interface, so those types can be treated the same where        
  they overlap axis-wise. If an axis doesn't exist on an element in the set, then it is simply ignored.                                                
                                                                                                                                                       
  Now we may want to graph fields and interfaces: graph composite{name=Root} + { .fields{name=*}, .interfaces{name=*} }. We again use set notation     
   to build a result set from multiple operations. In this example, we're traversing the fields axis and the interface axis. And since we are          
  using the + operator, we are adding these results to the current result set.                                                                         
                                                                                                                                                       
  If we want to replace the contents of the current result with the next operation, then we use the / operator. If I say "graph                        
  composite{name=Root} / .fields{name=*}" my result set will only the fields of the Root composite.                                                    
                                                                                                                                                       
  That's as far as I've gotten so far. I think the main idea is that we're using name+dictionary as a way to express an operation. The dictionary      
  gives us a lot of flexibility in specifying options for that particular operation. We may find that we want more than + and /, but we can add        
  those as we discover them. Right now, this syntax is intended for the graph command only, but it may prove useful as a general querying              
  mechanism, so we'll have to think about how we show these result sets in a table.
```