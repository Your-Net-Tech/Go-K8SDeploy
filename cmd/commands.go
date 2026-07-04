package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"k8s-deploy/state"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Inicializa estrutura do projeto",
	RunE: func(cmd *cobra.Command, args []string) error {
		dirs := []string{
			filepath.Join(workDir, "manifests", projectName),
			filepath.Join(workDir, "builds", projectName),
			filepath.Join(workDir, "logs", projectName),
			filepath.Join(workDir, "source", projectName),
		}
		for _, d := range dirs {
			if err := os.MkdirAll(d, 0755); err != nil {
				return err
			}
		}
		fmt.Printf("Projeto %s inicializado em %s\n", projectName, workDir)
		return nil
	},
}

var planCmd = &cobra.Command{
	Use:   "plan",
	Short: "Mostra plano de execucao (dry-run)",
	RunE: func(cmd *cobra.Command, args []string) error {
		store, err := state.NewStore(filepath.Join(workDir, "state"))
		if err != nil {
			return err
		}
		defer store.Close()

		deps, _ := store.ListDeployments(projectName, 1)
		if len(deps) > 0 {
			fmt.Printf("Ultima revisao: %d (status: %s)\n", deps[0].Revision, deps[0].Status)
		} else {
			fmt.Println("Primeira revisao")
		}
		fmt.Printf("Projeto: %s\n", projectName)
		fmt.Println("Pipeline: validate -> build -> apply -> health-check -> rollback")
		return nil
	},
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Status do deploy atual",
	RunE: func(cmd *cobra.Command, args []string) error {
		store, err := state.NewStore(filepath.Join(workDir, "state"))
		if err != nil {
			return err
		}
		defer store.Close()

		deps, err := store.ListDeployments(projectName, 10)
		if err != nil {
			return err
		}
		if len(deps) == 0 {
			fmt.Println("Nenhum deploy encontrado")
			return nil
		}
		fmt.Printf("Projeto: %s\n\n", projectName)
		for _, d := range deps {
			marker := " "
			if d.Status == "success" {
				marker = "OK"
			} else if d.Status == "failed" || d.FinishedAt != nil {
				marker = "ER"
			}
			fmt.Printf("  [%s] rev %d: %s\n", marker, d.Revision, d.Status)
			if d.Error != "" {
				fmt.Printf("       erro: %s\n", d.Error)
			}
		}
		return nil
	},
}

var rollbackCmd = &cobra.Command{
	Use:   "rollback [revision]",
	Short: "Reverte para revisao",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		rev := 0
		if len(args) > 0 {
			fmt.Sscanf(args[0], "%d", &rev)
		}
		if rev == 0 {
			fmt.Println("Uso: k8s-deploy rollback <rev>")
			return nil
		}
		store, err := state.NewStore(filepath.Join(workDir, "state"))
		if err != nil {
			return err
		}
		defer store.Close()

		dep, err := store.GetDeployment(int64(rev))
		if err != nil {
			return fmt.Errorf("revisao %d nao encontrada: %w", rev, err)
		}
		fmt.Printf("Rollback para revisao %d\nManifests: %s\n", rev, dep.Manifests)
		fmt.Println("TODO: implement rollback real")
		return nil
	},
}

var logsCmd = &cobra.Command{
	Use:   "logs [tipo]",
	Short: "Logs do deploy",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		tipo := "all"
		if len(args) > 0 {
			tipo = args[0]
		}
		logDir := filepath.Join(workDir, "logs", projectName)
		fmt.Printf("Logs em: %s (tipo: %s)\n", logDir, tipo)
		entries, _ := os.ReadDir(logDir)
		if len(entries) == 0 {
			fmt.Println("Sem logs")
			return nil
		}
		for _, e := range entries {
			fmt.Printf("  %s\n", e.Name())
		}
		return nil
	},
}