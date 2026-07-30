# Reviewed this configuration files and all checks for credo v1.7.12

%{
  configs: [
    %{
      name: "default",
      files: %{
        included: [
          "lib/",
          "src/",
          "test/",
          "web/",
          "apps/*/lib/",
          "apps/*/src/",
          "apps/*/test/",
          "apps/*/web/"
        ],
        excluded: [~r"/_build/", ~r"/deps/", ~r"/node_modules/"]
      },
      plugins: [],
      strict: true,
      parse_timeout: 5000,
      color: true,
      checks: [
        #
        ## overrides for configuration defaults
        #
        {Credo.Check.Design.AliasUsage, [if_nested_deeper_than: 3]},

        #
        ## flow: opted-in controversial or disabled by default. There may be future default checks
        #
        {Credo.Check.Consistency.MultiAliasImportRequireUse, []},
        {Credo.Check.Design.SkipTestWithoutComment, []},
        {Credo.Check.Readability.BlockPipe, []},
        {Credo.Check.Readability.ImplTrue, []},
        {Credo.Check.Readability.MultiAlias, []},
        {Credo.Check.Readability.OneArityFunctionInPipe, []},
        {Credo.Check.Readability.OnePipePerLine, []},
        {Credo.Check.Readability.SeparateAliasRequire, []},
        {Credo.Check.Readability.SingleFunctionToBlockPipe, []},
        {Credo.Check.Readability.SinglePipe, []},
        {Credo.Check.Readability.StrictModuleLayout, []},
        {Credo.Check.Readability.WithCustomTaggedTuple, []},
        {Credo.Check.Refactor.AppendSingleItem, []},
        {Credo.Check.Refactor.DoubleBooleanNegation, []},
        {Credo.Check.Refactor.FilterReject, []},
        {Credo.Check.Refactor.IoPuts, []},
        {Credo.Check.Refactor.NegatedIsNil, []},
        {Credo.Check.Refactor.PassAsyncInTestCases, []},
        {Credo.Check.Refactor.PerceivedComplexity, []},
        {Credo.Check.Refactor.PipeChainStart, []},
        {Credo.Check.Refactor.RejectFilter, []},
        {Credo.Check.Refactor.UtcNowTruncate, []},
        {Credo.Check.Warning.ForbiddenModule, []},
        {Credo.Check.Warning.IoInspect, []},
        {Credo.Check.Warning.LeakyEnvironment, []},
        {Credo.Check.Warning.MapGetUnsafePass, []},
        {Credo.Check.Warning.MixEnv, []},

        #
        ## flow: opted-out
        #
        {Credo.Check.Readability.ModuleDoc, false}

        #
        ## disabled checks we want to opt-in
        #
        # {Credo.Check.Consistency.UnusedVariableNames, []} # 229 issues | _ -> _params
        # {Credo.Check.Design.DuplicatedCode, []} # 3 issues | copy paste
        # {Credo.Check.Refactor.ABCSize, []} # 26 issues | complexity metric
        # {Credo.Check.Refactor.VariableRebinding, []} # 35 issues | no override of var
        # {Credo.Check.Warning.UnsafeToAtom, []} # 39 issues | binary_to_string_atom > binary_to_atom

        #
        ## disabled checks we have to discuss
        #
        # {Credo.Check.Readability.AliasAs, []} # 22 issues | no :as in alias
        # {Credo.Check.Readability.NestedFunctionCalls, []} # 291 issues | no b(a())
        # {Credo.Check.Refactor.MapMap, []} # 5 issues | no use of Enum.map/2 |> Enum.map/2
        # {Credo.Check.Refactor.ModuleDependencies, []} # 56 issues | no more than 10 deps

        #
        ## disabled checks we definitely don't want
        #
        # {Credo.Check.Readability.Specs, []}

        #
        ## deprecated
        #
        # {Credo.Check.Readability.PreferUnquotedAtoms, []}
        # {Credo.Check.Refactor.CaseTrivialMatches, []}
        # {Credo.Check.Refactor.MapInto, []}
        # {Credo.Check.Warning.LazyLogging, []}
      ]
    }
  ]
}
