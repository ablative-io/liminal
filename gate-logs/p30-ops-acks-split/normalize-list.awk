#!/usr/bin/awk -f
# Tag every `--list` test line with the suite binary that emitted it, so the
# before/after set comparison is suite-aware (two suites may hold the same
# test name; an untagged sort would silently merge them).
/^[ \t]*Running / {
    bin = $0
    sub(/^[ \t]*Running[ \t]+/, "", bin)
    # strip the crate/target prefix noise, keep the unittests/tests target path
    suite = bin
    next
}
/^[ \t]*Doc-tests / {
    suite = $0
    sub(/^[ \t]*/, "", suite)
    next
}
/: test$/ {
    name = $0
    sub(/: test$/, "", name)
    print suite "\t" name
    next
}
/: benchmark$/ {
    name = $0
    sub(/: benchmark$/, "", name)
    print suite "\tBENCH " name
    next
}
