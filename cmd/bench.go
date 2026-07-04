package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"k8s-deploy/internal/benchmark"
	"k8s-deploy/internal/stress"
)

var benchCmd = &cobra.Command{
	Use:   "bench",
	Short: "Benchmark suite (latencia + throughput do sistema)",
	RunE: func(cmd *cobra.Command, args []string) error {
		scenario, _ := cmd.Flags().GetString("scenario")
		switch scenario {
		case "db":
			benchmark.RunBenchmarks()
		case "stress":
			stress.RunAll()
		case "all":
			benchmark.RunBenchmarks()
			stress.RunAll()
		default:
			stress.RunAll()
		}
		return nil
	},
}

func init() {
	benchCmd.Flags().StringP("scenario", "s", "stress", "Scenario: stress, db, all")
	rootCmd.AddCommand(benchCmd)
}

func init() {
	fmt.Println("Iniciando benchmark suite...")
}