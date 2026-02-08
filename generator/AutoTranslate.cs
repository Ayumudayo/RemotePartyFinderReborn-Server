using System.Collections.Generic;
using System.Linq;
using Pidgin;
using static Pidgin.Parser;
using static Pidgin.Parser<char>;

namespace SourceGenerator; 

internal static class AutoTranslate {
    internal static Parser<char, (string name, Maybe<IEnumerable<ISelectorPart>> selector)> Parser() {
        var sheetName = Any
            .AtLeastOnceUntil(Lookahead(Char('[').IgnoreResult().Or(End)))
            .Select(string.Concat)
            .Labelled("sheetName");

        var numPair = Map(
                (first, second) => (ISelectorPart) new IndexRange(
                    uint.Parse(string.Concat(first)),
                    uint.Parse(string.Concat(second))
                ),
                Digit.AtLeastOnce().Before(Char('-')),
                Digit.AtLeastOnce()
            )
            .Labelled("numPair");
        var singleRow = Digit
            .AtLeastOnce()
            .Select(string.Concat)
            .Select(num => (ISelectorPart) new SingleRow(uint.Parse(num)));
        var column = String("col-")
            .Then(Digit.AtLeastOnce().Optional())
            .Select(num => num.HasValue 
                ? (ISelectorPart) new ColumnSpecifier(uint.Parse(string.Concat(num.Value)))
                : (ISelectorPart) new ColumnSpecifier(0));
        var noun = String("noun")
            .Select(_ => (ISelectorPart) new NounMarker());

        var selectorItem = OneOf(
                Try(numPair),
                singleRow,
                Try(column),
                noun
            )
            .Labelled("selectorItem");
        
        // Allow optional leading comma, then parse items separated by commas
        var selectorItems = Char(',').Then(Whitespace.Optional()).Optional()
            .Then(selectorItem.Separated(Char(',').Then(Whitespace.Optional())))
            .Select(items => items as IEnumerable<ISelectorPart>)
            .Labelled("selectorItems");
        
        // Handle empty brackets [] or brackets with content
        var selector = OneOf(
                selectorItems.Between(Char('['), Char(']')),
                Char('[').Then(Char(']')).Select(_ => Enumerable.Empty<ISelectorPart>())
            )
            .Optional()
            .Select(maybe => maybe.HasValue 
                ? Maybe.Just(maybe.Value) 
                : Maybe.Nothing<IEnumerable<ISelectorPart>>())
            .Labelled("selector");

        return Map(
            (name, selector) => (name, selector),
            sheetName,
            selector
        );
    }
}

internal interface ISelectorPart {
}

internal class SingleRow : ISelectorPart {
    public uint Row { get; }

    public SingleRow(uint row) {
        this.Row = row;
    }
}

internal class IndexRange : ISelectorPart {
    public uint Start { get; }
    public uint End { get; }

    public IndexRange(uint start, uint end) {
        this.Start = start;
        this.End = end;
    }
}

internal class NounMarker : ISelectorPart {
}

internal class ColumnSpecifier : ISelectorPart {
    public uint Column { get; }

    public ColumnSpecifier(uint column) {
        this.Column = column;
    }
}