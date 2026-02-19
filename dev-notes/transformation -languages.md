Established Graph Transformation Languages                                                                                                                      

AGG (Attributed Graph Grammars) — algebraic approach using graph rewriting rules. Each rule has a left-hand side (pattern to match) and right-hand side         
(replacement). Very formal but powerful for structural transformations.

GrGen.NET — probably the most mature graph rewriting tool. Rules are pattern-match → rewrite pairs. It has a dedicated language (GrGen) for specifying graph
patterns and transformations, plus a control flow language for sequencing rules. Worth studying for its separation of concerns.

PROGRES — programmed graph replacement systems. Introduced the idea of embedding graph transformations in a programming language with control structures.

Henshin — based on EMF (Eclipse Modeling Framework). Uses "transformation units" that compose rules with control flow (sequential, conditional, loops).

Model Transformation Languages (closer to your use case)

ATL (Atlas Transformation Language) — designed specifically for model-to-model transformation. Has a declarative style where you write "matched rules" that map
source patterns to target patterns. Very relevant to what you're describing.

QVT (Query/View/Transformation) — OMG standard with three sub-languages:
- QVT-Relations — declarative, bidirectional mappings between schemas
- QVT-Operational — imperative transformations
- QVT-Core — minimal foundational language

TGG (Triple Graph Grammars) — this one is particularly interesting for your case. It defines transformations using three connected graphs: source,
correspondence, and target. The correspondence graph explicitly captures the mapping between source and target elements. This cleanly separates "input schema"
from "output schema" concerns.

XSL-T — you already mentioned it. The key insight from XSLT is the template-matching paradigm: you declare rules that match patterns in the input and emit
corresponding output. The processor walks the input and applies matching templates.

Most Relevant to Your Situation

Your problem statement maps closest to ATL and TGG:

1. You have a source schema (the database types)
2. You want a target schema (nodes/edges, table columns, CSV, JSON, etc.)
3. You need mapping rules that describe how source elements become target elements

The TGG approach of an explicit correspondence layer might solve your concern about mixing input and output expressions. Instead of one expression language
trying to reference both schemas, you'd have:

source pattern  →  correspondence  →  target pattern

The ATL approach is more practical — rules look like:

rule PersonToNode {
    from s : Source!Person
    to   t : Target!Node (
        label <- s.name,
        kind  <- "person"
    )
}

The XSLT Angle

Your XSLT intuition is sound. The key ideas that transfer:
- Templates match input patterns, not output patterns
- Output construction is separate from input matching
- Apply-templates recursively processes children (composability)
- The processor drives the traversal, not the user

This avoids the confusion problem because expressions on the left side of a rule always reference the source, and expressions on the right always construct the
target.