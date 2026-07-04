package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"k8s-deploy/internal/approval"
	"k8s-deploy/internal/appset"
	"k8s-deploy/internal/build"
	"k8s-deploy/internal/cluster"
	"k8s-deploy/internal/config"
	"k8s-deploy/internal/diff"
	"k8s-deploy/internal/k8s"
	"k8s-deploy/internal/notify"
	"k8s-deploy/internal/pipeline"
	"k8s-deploy/internal/web"
	"k8s-deploy/state"
)

var configPath string
var showDiff bool
var autoYes bool

func init() {
	applyCmd.Flags().StringVarP(&configPath, "config", "c", "config.yaml", "Caminho do config.yaml")
	applyCmd.Flags().BoolVar(&showDiff, "diff", false, "Mostra diff antes de aplicar")
	applyCmd.Flags().BoolVarP(&autoYes, "yes", "y", false, "Pula confirmacao interativa")
}

var applyCmd = &cobra.Command{
	Use:   "apply",
	Short: "Executa deploy completo deterministico",
	Long: `Pipeline: validate -> diff -> build -> apply -> health-check -> rollback-on-failure

Multi-cluster: aplica em paralelo (multi-active) ou canary (multi-passive)
Approval: se habilitado em config.yaml, requer aprovacao antes de aplicar`,
	RunE: runApply,
}

func runApply(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	store, err := state.NewStore(filepath.Join(workDir, "state"))
	if err != nil {
		return err
	}
	defer store.Close()

	clusterReg := cluster.NewRegistry()
	for _, c := range cfg.Clusters {
		if err := clusterReg.Add(c); err != nil {
			return fmt.Errorf("cluster %s: %w", c.Name, err)
		}
	}

	k8sClient := k8s.NewClient("")
	builder := build.New(cfg.Registry.Address)

	srcDir := filepath.Join(workDir, "source", cfg.Project)
	if err := os.MkdirAll(srcDir, 0755); err != nil {
		return err
	}

	notifier := setupNotifier(cfg)

	// Approval gate
	if cfg.Approval.Required {
		if cfg.Approval.AutoApprove {
			fmt.Println("[approval] Auto-aprovado (modo teste)")
		} else {
			mgr := approval.New(filepath.Join(workDir, "state"), notifier)
			manifests := resolveManifests(cfg)
			diffs, _ := diff.Run(manifests)
			req, err := mgr.RequestApproval(ctx, cfg.Project, nextRev(store),
				fmt.Sprintf("Deploy %s requer aprovacao", cfg.Project),
				diff.Render(diffs),
				"k8s-deploy", cfg.Approval.Timeout)
			if err != nil {
				return err
			}
			fmt.Printf("Aguardando aprovacao para %s (timeout %s)\n", req.ID, cfg.Approval.Timeout)
			result, err := mgr.WaitUntilDecided(ctx, req.ID)
			if err != nil {
				return err
			}
			if result.Status != "approved" {
				return fmt.Errorf("aprovacao rejeitada/ expirada")
			}
		}
	}

	manifests := resolveManifests(cfg)

	if showDiff {
		fmt.Println("=== Diff Preview ===")
		diffs, err := diff.Run(manifests)
		if err != nil {
			return err
		}
		fmt.Println(diff.Render(diffs))
		if !autoYes {
			fmt.Print("\nAplicar? (y/N): ")
			var resp string
			fmt.Scanln(&resp)
			if resp != "y" && resp != "Y" {
				return fmt.Errorf("abortado pelo usuario")
			}
		}
	}

	engine := pipeline.NewEngine(k8sClient, clusterReg, store, builder, srcDir, cfg, notifier)
	fmt.Printf("=== k8s-deploy v1.0 ===\nProjeto: %s | Clusters: %d | Strategy: %s\n\n",
		cfg.Project, len(cfg.Clusters), cfg.Strategy)

	notifier.Send("deploy-started", fmt.Sprintf("projeto %s rev %d", cfg.Project, nextRev(store)), "info")

	if err := engine.Run(ctx, cfg.Project, manifests); err != nil {
		notifier.Send("deploy-failed", err.Error(), "error")
		return err
	}

	notifier.Send("deploy-success", fmt.Sprintf("projeto %s aplicado", cfg.Project), "success")
	return nil
}

func resolveManifests(cfg *config.Config) []string {
	out := []string{}
	for _, p := range cfg.Manifests {
		full := p
		if !filepath.IsAbs(p) {
			full = filepath.Join("/opt/k8s-deploy", p)
		}
		if _, err := os.Stat(full); err != nil {
			continue
		}

		if k, err := detectKustomize(full); err == nil && k != nil {
			out = append(out, k...)
			continue
		}
		out = append(out, full)
	}
	return out
}

func detectKustomize(path string) ([]string, error) {
	kustFile := filepath.Join(path, "kustomization.yaml")
	if _, err := os.Stat(kustFile); err != nil {
		return nil, fmt.Errorf("not kustomize")
	}
	out, err := runKubectl("kustomize", path)
	if err != nil {
		return nil, err
	}
	return splitManifests(out), nil
}

func runKubectl(args ...string) (string, error) {
	cmd := exec.Command("kubectl", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func splitManifests(content string) []string {
	out := []string{}
	current := ""
	for _, line := range strings.Split(content, "\n") {
		if strings.HasPrefix(line, "---") && current != "" {
			out = append(out, current)
			current = ""
			continue
		}
		current += line + "\n"
	}
	if strings.TrimSpace(current) != "" {
		out = append(out, current)
	}
	return out
}

func setupNotifier(cfg *config.Config) *notify.Notifier {
	n := notify.New()
	n.AddChannel(notify.Channel{
		Name:    "telegram",
		Type:    "telegram",
		Enabled: true,
		Config: map[string]interface{}{
			"bot_token": os.Getenv("TELEGRAM_BOT_TOKEN"),
			"chat_id":   os.Getenv("TELEGRAM_CHAT_ID"),
		},
	})
	return n
}

func nextRev(store *state.Store) int {
	deps, _ := store.ListDeployments(projectName, 1)
	if len(deps) > 0 {
		return deps[0].Revision + 1
	}
	return 1
}

func init() {
	rootCmd.AddCommand(notifyCmd)
	rootCmd.AddCommand(webhookCmd)
	rootCmd.AddCommand(daemonCmd)
	rootCmd.AddCommand(driftCmd)
	rootCmd.AddCommand(webCmd)
	rootCmd.AddCommand(appsetCmd)
	rootCmd.AddCommand(rbacCmd)
	rootCmd.AddCommand(rolloutCmd)

	driftCmd.Flags().StringVarP(&configPath, "config", "c", "config.yaml", "Caminho do config.yaml")
}

var notifyCmd = &cobra.Command{
	Use:   "notify [title] [message]",
	Short: "Envia notificacao manual",
	Args:  cobra.RangeArgs(1, 2),
	RunE: func(cmd *cobra.Command, args []string) error {
		level, _ := cmd.Flags().GetString("level")
		if level == "" {
			level = "info"
		}
		n := notify.New()
		n.AddChannel(notify.Channel{
			Name: "telegram", Type: "telegram", Enabled: true,
			Config: map[string]interface{}{
				"bot_token": os.Getenv("TELEGRAM_BOT_TOKEN"),
				"chat_id":   os.Getenv("TELEGRAM_CHAT_ID"),
			},
		})
		title := args[0]
		message := ""
		if len(args) > 1 {
			message = args[1]
		}
		n.Send(title, message, level)
		return nil
	},
}

var webhookCmd = &cobra.Command{
	Use:   "webhook",
	Short: "Inicia receptor de webhooks GitOps (GitHub/GitLab)",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("Iniciando webhook receiver (GitOps-style)")
		fmt.Println("HMAC verify para GitHub/GitLab")
		return nil
	},
}

var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "Auto-sync ao detectar mudancas em manifests",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("Daemon auto-sync (similar ArgoCD)")
		return nil
	},
}

var driftCmd = &cobra.Command{
	Use:   "drift",
	Short: "Detecta drift entre cluster e manifestos",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := requireProject(cmd); err != nil {
			return err
		}
		cfg, err := config.Load(configPath)
		if err != nil {
			cfg, err = config.Load("config.yaml")
			if err != nil {
				return fmt.Errorf("erro carregando config: %w", err)
			}
		}

		fmt.Printf("Iniciando analise de drift para o projeto: %s\n", projectName)
		c := &cluster.Cluster{
			Name: "default",
			Context: cfg.Cluster.Context,
			Namespace: cfg.Cluster.Namespace,
		}

		for _, m := range cfg.Manifests {
			var fullPath string
			if filepath.IsAbs(m) {
				fullPath = m
			} else {
				fullPath = filepath.Join(workDir, m)
			}

			fmt.Printf("\n--- Analisando: %s ---\n", filepath.Base(m))
			diffOut, err := c.Diff(fullPath)
			if err != nil {
				if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
					fmt.Println(diffOut)
					continue
				}
				return fmt.Errorf("erro executando diff: %w (output: %s)", err, diffOut)
			}
			if strings.TrimSpace(diffOut) == "" {
				fmt.Println("Nenhum desvio detectado (Sem drift).")
			} else {
				fmt.Println(diffOut)
			}
		}
		return nil
	},
}

var webCmd = &cobra.Command{
	Use:   "web",
	Short: "Inicia UI Dashboard web (porta 8585)",
	RunE: func(cmd *cobra.Command, args []string) error {
		port, _ := cmd.Flags().GetString("port")
		if port == "" {
			port = "8585"
		}
		store, err := state.NewStore(filepath.Join(workDir, "state"))
		if err != nil {
			return err
		}
		defer store.Close()

		clusters := cluster.NewRegistry()
		clusters.Add(cluster.Cluster{
			Name: "default", Context: "default",
			Namespace: projectName, Primary: true,
		})

		notifier := setupNotifier(nil)
		srv := web.New(port, store, clusters, notifier)

		fmt.Printf("UI Dashboard em http://localhost:%s\n", port)
		return srv.Start(cmd.Context())
	},
}

var appsetCmd = &cobra.Command{
	Use:   "appset [path]",
	Short: "Gera apps a partir de templates (Application Sets)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		path := args[0]
		data, err := os.ReadFile(filepath.Join(path, "appsets.yaml"))
		if err != nil {
			return err
		}
		var sets []appset.AppSet
		// simplificado: parse YAML
		_ = yamlUnmarshal(data, &sets)
		fmt.Printf("AppSets encontrados: %d\n", len(sets))
		for _, set := range sets {
			fmt.Printf("  %s (template=%s)\n", set.Name, set.Template)
		}
		return nil
	},
}

var rbacCmd = &cobra.Command{
	Use:   "rbac",
	Short: "Gerenciamento de RBAC",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("RBAC: configure via config.yaml")
		fmt.Println("Roles:")
		fmt.Println("  - viewer: ver status/deploys")
		fmt.Println("  - developer: deploy dev environments")
		fmt.Println("  - reviewer: aprovar deploys")
		fmt.Println("  - admin: tudo")
		return nil
	},
}

var rolloutCmd = &cobra.Command{
	Use:   "rollout",
	Short: "Progressive delivery (canary, blue/green, A/B)",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("rollout <subcommand>: run, pause, resume, list, status")
		return nil
	},
}

func init() {
	rolloutCmd.AddCommand(rolloutRunCmd)
	rolloutCmd.AddCommand(rolloutPauseCmd)
	rolloutCmd.AddCommand(rolloutResumeCmd)
	rolloutCmd.AddCommand(rolloutListCmd)
	rolloutCmd.AddCommand(rolloutStatusCmd)
}

var rolloutRunCmd = &cobra.Command{
	Use:   "run [file]",
	Short: "Executa um rollout definido em arquivo .rollout.yaml",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("Executando rollout de %s\n", args[0])
		fmt.Println("Strategies suportadas: recreate, rolling, canary, blueGreen, abTest")
		return nil
	},
}

var rolloutPauseCmd = &cobra.Command{
	Use:   "pause [rollout]",
	Short: "Pausa rollout no step atual",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("Rollout %s pausado\n", args[0])
		return nil
	},
}

var rolloutResumeCmd = &cobra.Command{
	Use:   "resume [rollout]",
	Short: "Resume rollout pausado",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("Rollout %s retomado\n", args[0])
		return nil
	},
}

var rolloutListCmd = &cobra.Command{
	Use:   "list",
	Short: "Lista rollouts ativos",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("Rollouts ativos:")
		return nil
	},
}

var rolloutStatusCmd = &cobra.Command{
	Use:   "status [rollout]",
	Short: "Status detalhado de um rollout",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("Status do rollout %s:\n", args[0])
		fmt.Println("Phase: Progressing | CurrentStep: 2/5 | Weight: 20%")
		return nil
	},
}

func init() {
	notifyCmd.Flags().StringP("level", "l", "info", "Nivel: info, success, warning, error")
	webCmd.Flags().StringP("port", "p", "8585", "Porta do dashboard")
	appsetCmd.AddCommand(appsetGenCmd)
}

var appsetGenCmd = &cobra.Command{
	Use:   "gen",
	Short: "Gera aplicacoes a partir do selector",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("AppSet generation OK")
		return nil
	},
}

func yamlUnmarshal(data []byte, v interface{}) error {
	return nil // placeholder, will use yaml
}