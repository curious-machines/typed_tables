# TODO

## Show References

We need to be able to write a query that will tell us everywhere a type is used. This applies to any type. For example, if I define a composite as "create type Person { name: string, age: float32 }", then a query for where float32 is used should include "Person, age" in the result.

I'd also like to be able to see a graph of every single reference, from a type perspective, in the database. The graph would contain nodes for each and every type. An arrow will connect a type and point to the type that references it. The edge itself could be labeled with the name of the property that is of that type, but that may get too cluttered. This graph should be output as an SVG document or as a graphviz file.

I'd like to be able to dump this calculated graph into its own database, so we need nodes for each type, along with a description of what kind of type it is. Then we need an array of references to other types that use that. If we want a name for the edge, then we'll need a different edge type that has that name and then points back to the owning type. This could be handled by using a ttq script that builds the database. We would let the user choose the database name, so no use statement should be in the ttq script. Creating a svg or graphviz file of this database should create a similar graph as described earlier, so this would be an alternate way to create a reference graph for a database. Ask questions if you need for me to clarify this last point.

We should discuss anything that is vague in these descriptions.

## Executor as a VM

We should consider if we can approach the query executor as a type of vm. We would need to identify the atomic operations that are being performed repeatedly throughout the code base. Each of these operations would become an instruction in the VM.

We would need to figure out how information is passed between instructions. We could use a stack, which is pretty simple. We could have an notion of registers that store specific types of values.

This would require turning a query into a sequence of instructions; a mini-compiler of sorts. This will be useful for debugging and could serve as a way to distribute queries between multiple running instances.

## Compact Export Format

This will be used for backup and for sharing copies of databases between processes and other machines. It might be thought of as a serialization of the database.

Currently, we use mmap to load tables into memory. I assume the file size is a multiple of page size for efficiency. However, this wastes space if we want to send the database to another process or machine or if we want to backup the data. For these use cases, we would like to end up with a single file that is compacted and maybe even compressed. It should be easy to consume and our code will need to support recreating a database from that file.

Compacting the database will require updates to all references since records will change position in the database. It seems like this could be a useful tool to run from the command line. This tool would perform the compaction step, but not inline. I would expect it to write to a new database folder, preserving the original in case there is some sort of failure during the compaction step.

Some questions: Does dump already meet this requirement? Would a binary version be smaller?

This may be a pretty large feature, so lets discuss pros and cons and possible alternate implementations.

---

# Completed

## Default Values

Currently, if a property is not set during instantiation, the value defaults to NULL. We should be able to set what the default value should be for fields that aren't defined at instantiation. I suggest adding "= <value>" after the property definition when defining the type. The <value> has to match the type of the property. This default value is not required and in that case will default to NULL, matching the current behavior. I assume this default value will be stored in _metadata.json which means that the value need to representable in JSON. For example, int128/uint128 is not representable as a number in JSON, so we would have to switch to storing that value as a string.

## No Referents When Selecting a Primitive or Alias

When primitive values (and maybe aliases too) are selected in the REPL, we get a list of indexes and values. This doesn't show us from where the value came. The output should include the name of the type using the value and the name of the property from where the value came. This may be a regression.

## Storing Sum Types Members in Tables

We decided to store sum types inline in each record. Properties have to be of the same size, so we find the longest sized sum type member and make all other members use that same size. This likely leads to wasted space in the database.

Something doesn't feel right about how we're storing these values. To me, each variant is really a new type; therefore, I think each member should be stored in its own table. Now the property that uses that sum type will need a discriminant and an index in order to know which table to use and at what index the value is stored.

One concern is that we will have a lot of tables if there are a lot of sum types with a lot of members. In order to reduce the clutter, I propose that we create a folder named after the sum type, then each of the member tables will reside in that folder, named by their member name.

Something to consider. If we use a plain enumeration, then it may be better to store its numeric value directly in the record. So, this separating of member types into separate tables would only apply to sum type enumerations.

This is a big change, so we should discuss pros and cons and possible alternate solutions
