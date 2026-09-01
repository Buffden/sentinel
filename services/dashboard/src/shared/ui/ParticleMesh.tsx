'use client'

import { useEffect, useRef } from 'react'

const COLS = 26
const ROWS = 16

// Floating coordinate labels scattered around the canvas (relative positions 0–1)
const FLOAT_LABELS = [
	{ rx: 0.07, ry: 0.32, v: '72.993' },
	{ rx: 0.03, ry: 0.50, v: '186.7749' },
	{ rx: 0.09, ry: 0.63, v: '112.9786' },
	{ rx: 0.19, ry: 0.25, v: '189.5963' },
	{ rx: 0.26, ry: 0.60, v: '188.7851' },
	{ rx: 0.41, ry: 0.18, v: '91.1290' },
	{ rx: 0.56, ry: 0.12, v: '46.8016' },
	{ rx: 0.62, ry: 0.75, v: '42.2131' },
	{ rx: 0.71, ry: 0.22, v: '96.8458' },
	{ rx: 0.82, ry: 0.15, v: '100.4703' },
	{ rx: 0.86, ry: 0.48, v: '37.8821' },
	{ rx: 0.91, ry: 0.65, v: '55.1204' },
	{ rx: 0.15, ry: 0.80, v: '203.0091' },
	{ rx: 0.75, ry: 0.82, v: '67.4412' },
]

export default function ParticleMesh() {
	const canvasRef = useRef<HTMLCanvasElement>(null)

	useEffect(() => {
		const canvas = canvasRef.current
		if (!canvas) return
		const ctx = canvas.getContext('2d')
		if (!ctx) return

		let animId: number
		let t = 0

		function resize() {
			if (!canvas) return
			canvas.width = window.innerWidth
			canvas.height = window.innerHeight
		}
		resize()
		window.addEventListener('resize', resize)

		function draw() {
			if (!canvas || !ctx) return
			const W = canvas.width
			const H = canvas.height

			ctx.clearRect(0, 0, W, H)

			// Build particle positions for this frame
			const padX = W * 0.0
			const padY = H * 0.05
			const spacingX = (W - padX * 2) / (COLS - 1)
			const spacingY = (H - padY * 2) / (ROWS - 1)

			const pts: { x: number; y: number; alpha: number }[] = []

			for (let j = 0; j < ROWS; j++) {
				for (let i = 0; i < COLS; i++) {
					const baseX = padX + i * spacingX
					const baseY = padY + j * spacingY

					// Multi-frequency wave giving organic shape
					const amp = H * 0.06
					const dy =
						Math.sin(i * 0.38 + t) * Math.cos(j * 0.28 + t * 0.65) * amp +
						Math.sin(i * 0.18 - t * 0.5 + j * 0.1) * amp * 0.45 +
						Math.cos(i * 0.55 + j * 0.42 - t * 0.8) * amp * 0.25

					// Fade particles at the very edges
					const edgeFadeX = Math.min(i / 2, (COLS - 1 - i) / 2, 1)
					const edgeFadeY = Math.min(j / 1.5, (ROWS - 1 - j) / 1.5, 1)
					const alpha = Math.min(edgeFadeX, edgeFadeY)

					pts.push({ x: baseX, y: baseY + dy, alpha })
				}
			}

			// Draw grid connections
			for (let j = 0; j < ROWS; j++) {
				for (let i = 0; i < COLS; i++) {
					const idx = j * COLS + i
					const p = pts[idx]

					// Horizontal connection
					if (i < COLS - 1) {
						const q = pts[idx + 1]
						const a = Math.min(p.alpha, q.alpha) * (0.10 + 0.05 * Math.sin(i * 0.4 + t))
						ctx.strokeStyle = `rgba(255,255,255,${a.toFixed(3)})`
						ctx.lineWidth = 0.6
						ctx.beginPath()
						ctx.moveTo(p.x, p.y)
						ctx.lineTo(q.x, q.y)
						ctx.stroke()
					}

					// Vertical connection
					if (j < ROWS - 1) {
						const q = pts[idx + COLS]
						const a = Math.min(p.alpha, q.alpha) * (0.10 + 0.05 * Math.sin(j * 0.35 + t * 0.9))
						ctx.strokeStyle = `rgba(255,255,255,${a.toFixed(3)})`
						ctx.lineWidth = 0.6
						ctx.beginPath()
						ctx.moveTo(p.x, p.y)
						ctx.lineTo(q.x, q.y)
						ctx.stroke()
					}
				}
			}

			// Draw node dots
			for (const p of pts) {
				const a = p.alpha * 0.55
				ctx.beginPath()
				ctx.arc(p.x, p.y, 1.3, 0, Math.PI * 2)
				ctx.fillStyle = `rgba(255,255,255,${a.toFixed(3)})`
				ctx.fill()
			}

			// Draw floating coordinate labels
			ctx.font = '11px monospace'
			for (const n of FLOAT_LABELS) {
				ctx.fillStyle = 'rgba(255,255,255,0.22)'
				ctx.fillText(n.v, n.rx * W, n.ry * H)
			}

			t += 0.007
			animId = requestAnimationFrame(draw)
		}

		draw()

		return () => {
			cancelAnimationFrame(animId)
			window.removeEventListener('resize', resize)
		}
	}, [])

	return (
		<canvas
			ref={canvasRef}
			style={{
				position: 'absolute',
				inset: 0,
				width: '100%',
				height: '100%',
				pointerEvents: 'none',
			}}
		/>
	)
}
